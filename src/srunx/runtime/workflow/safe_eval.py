"""AST-walker sandbox for ``python:`` prefixed YAML args.

This module implements a restricted Python expression / statement evaluator
used by the workflow loader to resolve ``python: <expr>`` args declared at
workflow load time. It parses user source into an AST and interprets it
directly — ``eval`` / ``exec`` are never invoked, which prevents sandbox
escape via ``__class__`` / ``__subclasses__``.

Carved out of :mod:`srunx.runtime.workflow.runner` unchanged as part of Phase 7 (#163).
"""

import ast
import datetime
import math
from typing import Any


def _safe_eval(code: str, local_vars: dict[str, Any]) -> Any:
    """Evaluate a Python expression safely.

    Only allows:
    - Literal values (strings, numbers, lists, dicts, tuples, booleans, None)
    - Simple function calls from a small allowlist (datetime.date, math.ceil, etc.)
    - Variable references from local_vars

    This does NOT use eval()/exec() — it parses the AST and interprets it directly,
    making sandbox escape via __class__/__subclasses__ impossible.
    """
    tree = ast.parse(code.strip(), mode="eval")
    return _eval_node(tree.body, local_vars)


def _safe_exec(code: str, local_vars: dict[str, Any]) -> dict[str, Any]:
    """Execute simple Python assignments safely.

    Only supports: `result = <expression>` style assignments.
    """
    tree = ast.parse(code.strip(), mode="exec")
    ns = dict(local_vars)
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                ns[target.id] = _eval_node(node.value, ns)
            else:
                raise ValueError(f"Unsupported assignment target: {ast.dump(target)}")
        elif isinstance(node, ast.Expr):
            _eval_node(node.value, ns)
        else:
            raise ValueError(
                f"Unsupported statement in python: arg: {ast.dump(node)}. "
                "Only simple assignments (result = ...) are allowed."
            )
    return ns


# Allowlisted callable functions for python: args
_SAFE_CALLABLES: dict[str, Any] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
    "tuple": tuple,
    "len": len,
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "sum": sum,
    "sorted": sorted,
    "range": range,
    "format": format,
    "repr": repr,
}

# Allowlisted module attributes
_SAFE_MODULES: dict[str, Any] = {
    "datetime": datetime,
    "math": math,
}


def _eval_node(node: ast.AST, local_vars: dict[str, Any]) -> Any:  # noqa: C901, PLR0911, PLR0912
    """Recursively evaluate an AST node in a safe context."""
    # Literals
    if isinstance(node, ast.Constant):
        return node.value

    # Variable lookup
    if isinstance(node, ast.Name):
        if node.id in local_vars:
            return local_vars[node.id]
        if node.id in _SAFE_CALLABLES:
            return _SAFE_CALLABLES[node.id]
        if node.id in _SAFE_MODULES:
            return _SAFE_MODULES[node.id]
        if node.id in ("True", "False", "None"):
            return {"True": True, "False": False, "None": None}[node.id]
        raise NameError(f"Name '{node.id}' is not allowed in python: args")

    # Attribute access (only on allowlisted modules)
    if isinstance(node, ast.Attribute):
        obj = _eval_node(node.value, local_vars)
        # Only allow attribute access on allowlisted modules and their results
        if obj in _SAFE_MODULES.values() or type(obj).__module__ in (
            "datetime",
            "math",
        ):
            return getattr(obj, node.attr)
        raise AttributeError(f"Attribute access on {type(obj).__name__} is not allowed")

    # Function calls
    if isinstance(node, ast.Call):
        func = _eval_node(node.func, local_vars)
        args = [_eval_node(a, local_vars) for a in node.args]
        kwargs = {
            kw.arg: _eval_node(kw.value, local_vars) for kw in node.keywords if kw.arg
        }
        return func(*args, **kwargs)

    # Subscript (e.g., args['x'], list[0])
    if isinstance(node, ast.Subscript):
        obj = _eval_node(node.value, local_vars)
        key = _eval_node(node.slice, local_vars)
        return obj[key]

    # Binary ops
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, local_vars)
        right = _eval_node(node.right, local_vars)
        ops = {
            ast.Add: lambda a, b: a + b,
            ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div: lambda a, b: a / b,
            ast.FloorDiv: lambda a, b: a // b,
            ast.Mod: lambda a, b: a % b,
            ast.Pow: lambda a, b: a**b,
        }
        op_func = ops.get(type(node.op))
        if op_func:
            return op_func(left, right)
        raise ValueError(f"Unsupported operator: {type(node.op).__name__}")

    # Unary ops
    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, local_vars)
        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.Not):
            return not operand
        raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

    # Compare ops
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, local_vars)
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            right = _eval_node(comparator, local_vars)
            cmp_ops = {
                ast.Eq: lambda a, b: a == b,
                ast.NotEq: lambda a, b: a != b,
                ast.Lt: lambda a, b: a < b,
                ast.LtE: lambda a, b: a <= b,
                ast.Gt: lambda a, b: a > b,
                ast.GtE: lambda a, b: a >= b,
                ast.In: lambda a, b: a in b,
                ast.NotIn: lambda a, b: a not in b,
            }
            op_func = cmp_ops.get(type(op))
            if not op_func:
                raise ValueError(f"Unsupported comparison: {type(op).__name__}")
            if not op_func(left, right):
                return False
            left = right
        return True

    # Boolean ops
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(_eval_node(v, local_vars) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(_eval_node(v, local_vars) for v in node.values)

    # Ternary (a if cond else b)
    if isinstance(node, ast.IfExp):
        if _eval_node(node.test, local_vars):
            return _eval_node(node.body, local_vars)
        return _eval_node(node.orelse, local_vars)

    # List/Tuple/Set/Dict comprehensions and literals
    if isinstance(node, ast.List):
        return [_eval_node(e, local_vars) for e in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(e, local_vars) for e in node.elts)
    if isinstance(node, ast.Set):
        return {_eval_node(e, local_vars) for e in node.elts}
    if isinstance(node, ast.Dict):
        return {
            _eval_node(k, local_vars): _eval_node(v, local_vars)
            for k, v in zip(node.keys, node.values, strict=True)
            if k is not None
        }

    # f-string (JoinedStr)
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                parts.append(str(_eval_node(v.value, local_vars)))
            else:
                parts.append(str(_eval_node(v, local_vars)))
        return "".join(parts)

    raise ValueError(
        f"Unsupported expression in python: arg: {ast.dump(node)}. "
        "Only literals, simple function calls, and basic operations are allowed."
    )
