"""
Simulator/Commands.py

DAS-like "custom command" scripting support:
- Parse scripts like: "lmtprice = ask - .10; shares=5000 / lmtprice; BUY;"
- Validate syntax + allowed identifiers/commands
- Provide helpers to load/save Configs/commands.json

This module is intentionally backend-only (FastAPI uses it for validation + persistence).
Execution (variable read/write + order actions) is implemented in the browser UI.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


# ---------------- Errors ----------------


@dataclass(frozen=True)
class ScriptError:
    message: str
    index: int  # 0-based char offset into the original string
    line: int  # 1-based
    col: int  # 1-based


class ScriptSyntaxError(ValueError):
    def __init__(self, message: str, index: int, line: int, col: int):
        super().__init__(message)
        self.message = message
        self.index = index
        self.line = line
        self.col = col


def _line_col_from_index(s: str, idx: int) -> Tuple[int, int]:
    idx = max(0, min(len(s), int(idx)))
    line = 1
    col = 1
    for i, ch in enumerate(s):
        if i >= idx:
            break
        if ch == "\n":
            line += 1
            col = 1
        else:
            col += 1
    return line, col


# ---------------- AST ----------------


@dataclass(frozen=True)
class Expr:
    pass


@dataclass(frozen=True)
class Number(Expr):
    value: float


@dataclass(frozen=True)
class Var(Expr):
    name: str


@dataclass(frozen=True)
class StringLit(Expr):
    value: str


@dataclass(frozen=True)
class GetVarCall(Expr):
    name: str


@dataclass(frozen=True)
class UnaryOp(Expr):
    op: str  # '+' | '-'
    rhs: Expr


@dataclass(frozen=True)
class BinOp(Expr):
    lhs: Expr
    op: str  # '+' | '-' | '*' | '/'
    rhs: Expr


@dataclass(frozen=True)
class Stmt:
    pass


@dataclass(frozen=True)
class Assign(Stmt):
    name: str
    expr: Expr


@dataclass(frozen=True)
class Command(Stmt):
    name: str  # BUY | SELL | CANCELALL


@dataclass(frozen=True)
class CallStmt(Stmt):
    name: str  # SETVAR | DELVAR
    args: List[Expr]


@dataclass(frozen=True)
class Script:
    stmts: List[Stmt]


# ---------------- Lexer ----------------


@dataclass(frozen=True)
class Tok:
    kind: str
    text: str
    i: int


_OPS = {"+", "-", "*", "/"}
_SEPS = {";", ","}


def _is_ident_start(ch: str) -> bool:
    return ch.isalpha() or ch in {"_", "$"}


def _is_ident_cont(ch: str) -> bool:
    return ch.isalnum() or ch in {"_", "$"}


def _lex(s: str) -> List[Tok]:
    out: List[Tok] = []
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if ch.isspace():
            i += 1
            continue
        if ch in _SEPS:
            out.append(Tok("SEP", ch, i))
            i += 1
            continue
        if ch == "(":
            out.append(Tok("LP", ch, i))
            i += 1
            continue
        if ch == ")":
            out.append(Tok("RP", ch, i))
            i += 1
            continue
        if ch == "=":
            out.append(Tok("EQ", ch, i))
            i += 1
            continue
        if ch in {"'", '"'}:
            q = ch
            j = i + 1
            buf: List[str] = []
            while j < n:
                cj = s[j]
                if cj == "\\" and j + 1 < n:
                    buf.append(s[j + 1])
                    j += 2
                    continue
                if cj == q:
                    out.append(Tok("STR", "".join(buf), i))
                    i = j + 1
                    break
                buf.append(cj)
                j += 1
            else:
                line, col = _line_col_from_index(s, i)
                raise ScriptSyntaxError("Unterminated string literal", i, line, col)
            continue
        if ch in _OPS:
            out.append(Tok("OP", ch, i))
            i += 1
            continue
        if ch.isdigit() or ch == ".":
            j = i
            saw_digit = False
            if s[j] == ".":
                # allow ".10" style numbers, but require a digit after dot
                j += 1
                if j >= n or not s[j].isdigit():
                    line, col = _line_col_from_index(s, i)
                    raise ScriptSyntaxError("Invalid number literal", i, line, col)
            while j < n and (s[j].isdigit() or s[j] == "."):
                if s[j].isdigit():
                    saw_digit = True
                j += 1
            txt = s[i:j]
            if not saw_digit:
                line, col = _line_col_from_index(s, i)
                raise ScriptSyntaxError("Invalid number literal", i, line, col)
            out.append(Tok("NUM", txt, i))
            i = j
            continue
        if _is_ident_start(ch):
            j = i + 1
            while j < n and _is_ident_cont(s[j]):
                j += 1
            out.append(Tok("IDENT", s[i:j], i))
            i = j
            continue
        line, col = _line_col_from_index(s, i)
        raise ScriptSyntaxError(f"Unexpected character '{ch}'", i, line, col)
    out.append(Tok("EOF", "", n))
    return out


# ---------------- Parser ----------------


READ_VARS = {
    "ask",
    "bid",
    "last",
    "hi",
    "lo",
    "open",
    "pcl",
    "l2bid",
    "l2ask",
    "buypower",
    "shares",
    "share",
    "ticker",
    "position",
    "pos",
    "lmtprice",
    "price",
    "stopprice",
    "stoptype",
    "route",
    "costbasis",
}
WRITE_VARS = {
    "shares",
    "share",
    "lmtprice",
    "price",
    "stopprice",
    "stoptype",
    "route",
}
COMMANDS = {"BUY", "SELL", "CANCELALL"}
CALL_STMTS = {"SETVAR", "DELVAR"}
CALL_EXPRS = {"GETVAR"}


class _P:
    def __init__(self, s: str, toks: Sequence[Tok]):
        self.s = s
        self.toks = toks
        self.k = 0

    def cur(self) -> Tok:
        return self.toks[self.k]

    def eat(self, kind: str) -> Tok:
        t = self.cur()
        if t.kind != kind:
            line, col = _line_col_from_index(self.s, t.i)
            raise ScriptSyntaxError(f"Expected {kind}, got {t.kind}", t.i, line, col)
        self.k += 1
        return t

    def maybe(self, kind: str) -> Optional[Tok]:
        if self.cur().kind == kind:
            t = self.cur()
            self.k += 1
            return t
        return None

    def _peek_kind(self) -> str:
        if self.k + 1 >= len(self.toks):
            return "EOF"
        return self.toks[self.k + 1].kind

    def parse_call_args(self) -> List[Expr]:
        self.eat("LP")
        args: List[Expr] = []
        if self.cur().kind == "RP":
            self.eat("RP")
            return args
        args.append(self.parse_expr())
        while self.cur().kind == "SEP" and self.cur().text == ",":
            self.k += 1
            args.append(self.parse_expr())
        self.eat("RP")
        return args

    def parse_script(self) -> Script:
        stmts: List[Stmt] = []
        # Allow leading seps
        while self.cur().kind == "SEP":
            self.k += 1
        while self.cur().kind != "EOF":
            stmts.append(self.parse_stmt())
            # 0+ separators between stmts (accepts trailing separators)
            while self.cur().kind == "SEP":
                self.k += 1
            if self.cur().kind == "EOF":
                break
        return Script(stmts=stmts)

    def parse_stmt(self) -> Stmt:
        t = self.cur()
        if t.kind == "IDENT":
            ident = t.text
            # Command keyword?
            if ident.upper() in COMMANDS:
                self.k += 1
                return Command(name=ident.upper())
            # Call statement? SetVar(...) / DelVar(...)
            if ident.upper() in CALL_STMTS and self._peek_kind() == "LP":
                fn = ident.upper()
                self.k += 1
                args = self.parse_call_args()
                if fn == "DELVAR" and len(args) != 1:
                    line, col = _line_col_from_index(self.s, t.i)
                    raise ScriptSyntaxError("DelVar expects 1 argument", t.i, line, col)
                if fn == "SETVAR" and len(args) != 2:
                    line, col = _line_col_from_index(self.s, t.i)
                    raise ScriptSyntaxError("SetVar expects 2 arguments", t.i, line, col)
                if not args or not isinstance(args[0], StringLit):
                    line, col = _line_col_from_index(self.s, t.i)
                    raise ScriptSyntaxError("First argument must be a string literal", t.i, line, col)
                return CallStmt(name=fn, args=args)
            # Assignment?
            self.k += 1
            if self.cur().kind != "EQ":
                line, col = _line_col_from_index(self.s, self.cur().i)
                raise ScriptSyntaxError("Expected '=' after identifier", self.cur().i, line, col)
            key = ident.lower()
            if not key.startswith("$") and key not in READ_VARS:
                line, col = _line_col_from_index(self.s, t.i)
                raise ScriptSyntaxError(f"Unknown variable '{ident}'", t.i, line, col)
            if not key.startswith("$") and key not in WRITE_VARS:
                line, col = _line_col_from_index(self.s, t.i)
                raise ScriptSyntaxError(f"Variable '{ident}' is read-only", t.i, line, col)
            self.eat("EQ")
            expr = self.parse_expr()
            return Assign(name=key, expr=expr)
        line, col = _line_col_from_index(self.s, t.i)
        raise ScriptSyntaxError("Expected a command or assignment", t.i, line, col)

    def parse_expr(self) -> Expr:
        node = self.parse_term()
        while self.cur().kind == "OP" and self.cur().text in {"+", "-"}:
            op = self.cur().text
            self.k += 1
            rhs = self.parse_term()
            node = BinOp(node, op, rhs)
        return node

    def parse_term(self) -> Expr:
        node = self.parse_factor()
        while self.cur().kind == "OP" and self.cur().text in {"*", "/"}:
            op = self.cur().text
            self.k += 1
            rhs = self.parse_factor()
            node = BinOp(node, op, rhs)
        return node

    def parse_factor(self) -> Expr:
        t = self.cur()
        if t.kind == "OP" and t.text in {"+", "-"}:
            op = t.text
            self.k += 1
            rhs = self.parse_factor()
            return UnaryOp(op, rhs)
        if t.kind == "STR":
            self.k += 1
            return StringLit(t.text)
        if t.kind == "NUM":
            self.k += 1
            try:
                v = float(t.text)
            except Exception:
                line, col = _line_col_from_index(self.s, t.i)
                raise ScriptSyntaxError("Invalid number literal", t.i, line, col)
            return Number(v)
        if t.kind == "IDENT":
            if t.text.upper() in CALL_EXPRS and self._peek_kind() == "LP":
                self.k += 1
                args = self.parse_call_args()
                if len(args) != 1:
                    line, col = _line_col_from_index(self.s, t.i)
                    raise ScriptSyntaxError("GetVar expects 1 argument", t.i, line, col)
                if not isinstance(args[0], StringLit):
                    line, col = _line_col_from_index(self.s, t.i)
                    raise ScriptSyntaxError("GetVar argument must be a string literal", t.i, line, col)
                return GetVarCall(name=args[0].value)
            self.k += 1
            name = t.text.lower()
            if not name.startswith("$") and name not in READ_VARS:
                line, col = _line_col_from_index(self.s, t.i)
                raise ScriptSyntaxError(f"Unknown variable '{t.text}'", t.i, line, col)
            return Var(name)
        if t.kind == "LP":
            self.k += 1
            node = self.parse_expr()
            self.eat("RP")
            return node
        line, col = _line_col_from_index(self.s, t.i)
        raise ScriptSyntaxError("Expected a number, variable, or '('", t.i, line, col)


def parse_script(text: str) -> Script:
    toks = _lex(text or "")
    p = _P(text or "", toks)
    script = p.parse_script()
    # Ensure no junk at end (parse_script already consumes separators)
    if p.cur().kind != "EOF":
        t = p.cur()
        line, col = _line_col_from_index(text or "", t.i)
        raise ScriptSyntaxError("Unexpected trailing input", t.i, line, col)
    return script


def validate_script(text: str) -> Dict[str, Any]:
    """
    Returns:
      {"ok": True}
    or:
      {"ok": False, "errors": [{"message","index","line","col"}...]}
    """
    try:
        parse_script(text or "")
        return {"ok": True}
    except ScriptSyntaxError as e:
        return {
            "ok": False,
            "errors": [
                {
                    "message": str(e),
                    "index": int(e.index),
                    "line": int(e.line),
                    "col": int(e.col),
                }
            ],
        }


# ---------------- Persistence ----------------


DEFAULT_COMMANDS_STATE: Dict[str, Any] = {"version": 1, "commands": []}


def load_commands(configs_dir: Path) -> Dict[str, Any]:
    p = Path(configs_dir) / "commands.json"
    if not p.exists():
        return json.loads(json.dumps(DEFAULT_COMMANDS_STATE))
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return json.loads(json.dumps(DEFAULT_COMMANDS_STATE))
        return data
    except Exception:
        return json.loads(json.dumps(DEFAULT_COMMANDS_STATE))


def save_commands(configs_dir: Path, data: Any) -> Path:
    Path(configs_dir).mkdir(parents=True, exist_ok=True)
    p = Path(configs_dir) / "commands.json"
    p.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    return p

