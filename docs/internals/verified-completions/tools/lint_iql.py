#!/usr/bin/env python3
"""Lint an IQL file against the rules verified from inputlayer/inputlayer docs:
- statement forms: schema decl, single/bulk fact insert, persistent rule
- consistent arity per relation across schemas, facts, rule heads/bodies
- variable safety: head vars, comparison vars, negation vars bound positively
- wildcard `_` only inside negated atoms
- aggregation terms count/sum/min/max/avg/count_distinct/top_k<...> in heads
- strings double-quoted; ints bare; identifiers snake_case; vars Uppercase
"""
import re, sys

AGG = re.compile(r'^(count|count_distinct|sum|min|max|avg|top_k)<.+>$')
CMP = re.compile(r'^\s*(\S+)\s*(!=|<=|>=|<|>|=)\s*(\S+)\s*$')

def split_top(s, sep=','):
    out, depth, cur, ins = [], 0, '', False
    for ch in s:
        if ch == '"': ins = not ins
        if not ins:
            if ch in '([<': depth += 1
            elif ch in ')]>': depth -= 1
            if ch == sep and depth == 0:
                out.append(cur.strip()); cur = ''; continue
        cur += ch
    if cur.strip(): out.append(cur.strip())
    return out

def parse_atom(s):
    m = re.match(r'^(!?)([a-z_][a-z0-9_]*)\((.*)\)$', s.strip(), re.S)
    if not m: return None
    neg, name, args = m.group(1) == '!', m.group(2), split_top(m.group(3))
    return neg, name, args

def term_vars(t):
    if t == '_' or t.startswith('"') or re.fullmatch(r'-?\d+(\.\d+)?', t): return set()
    if AGG.match(t): return set(re.findall(r'\b[A-Z][A-Za-z0-9_]*\b', t))
    return set(re.findall(r'\b[A-Z][A-Za-z0-9_]*\b', t))

errors, warnings, arity = [], [], {}

def see_arity(name, n, where):
    if name in arity and arity[name] != n:
        errors.append(f"{where}: relation '{name}' arity {n} != earlier {arity[name]}")
    arity.setdefault(name, n)

def lint(path):
    src = open(path).read()
    stmts = []
    for i, raw in enumerate(src.splitlines(), 1):
        line = re.sub(r'//.*$', '', raw).strip()
        if line: stmts.append((i, line))
    for i, s in stmts:
        w = f"line {i}"
        if s.startswith('.'):  # meta command
            continue
        if '<-' in s:
            head, body = [x.strip() for x in s.split('<-', 1)]
            if not head.startswith('+'):
                warnings.append(f"{w}: session (non-persistent) rule")
            h = parse_atom(head.lstrip('+'))
            if not h: errors.append(f"{w}: unparsable head: {head}"); continue
            _, hname, hargs = h
            see_arity(hname, len(hargs), w)
            pos_vars, lits = set(), split_top(body)
            atoms = []
            for lit in lits:
                a = parse_atom(lit)
                if a:
                    atoms.append((a, lit))
                    neg, name, args = a
                    see_arity(name, len(args), w)
                    if not neg:
                        for t in args: pos_vars |= term_vars(t)
                        if '_' in args: errors.append(f"{w}: wildcard outside negation: {lit}")
                elif CMP.match(lit): pass
                else: errors.append(f"{w}: unparsable body literal: {lit}")
            for lit in lits:
                a = parse_atom(lit)
                if a and a[0]:
                    for t in a[2]:
                        for v in term_vars(t):
                            if v not in pos_vars:
                                errors.append(f"{w}: negation var {v} unbound: {lit}")
                m = CMP.match(lit)
                if m and not parse_atom(lit):
                    l, op, r = m.groups()
                    bind = op == '=' and term_vars(l) and not (term_vars(l) & pos_vars)
                    for v in (term_vars(l) | term_vars(r)):
                        if bind and v in term_vars(l): pos_vars.add(v); continue
                        if v not in pos_vars:
                            errors.append(f"{w}: comparison var {v} unbound: {lit}")
            for t in hargs:
                if AGG.match(t):
                    for v in term_vars(t):
                        if v not in pos_vars: errors.append(f"{w}: agg var {v} unbound")
                    continue
                for v in term_vars(t):
                    if v not in pos_vars: errors.append(f"{w}: head var {v} unbound")
        elif s.startswith('+') and '[' in s and s.endswith(']'):
            m = re.match(r'^\+([a-z_][a-z0-9_]*)\[(.*)\]$', s, re.S)
            if not m: errors.append(f"{w}: unparsable bulk insert"); continue
            name, tuples = m.group(1), split_top(m.group(2))
            for tup in tuples:
                if not (tup.startswith('(') and tup.endswith(')')):
                    errors.append(f"{w}: bad tuple {tup} in bulk {name}"); continue
                args = split_top(tup[1:-1])
                see_arity(name, len(args), w)
                for t in args:
                    if term_vars(t): errors.append(f"{w}: variable in fact: {t}")
        elif s.startswith('+') or s.startswith('-'):
            a = parse_atom(s[1:])
            if not a: errors.append(f"{w}: unparsable statement: {s}"); continue
            _, name, args = a
            if all(':' in x for x in args) and args:   # schema declaration
                see_arity(name, len(args), w)
                for col in args:
                    cn, ct = [x.strip() for x in col.split(':', 1)]
                    if ct not in ('int', 'string', 'float', 'bool', 'symbol', 'timestamp', 'text', 'varchar'):
                        warnings.append(f"{w}: unusual type '{ct}' for {name}.{cn}")
            else:                                       # single fact
                see_arity(name, len(args), w)
                for t in args:
                    if term_vars(t): errors.append(f"{w}: variable in fact: {t}")
        elif s.startswith('?'):
            continue
        else:
            errors.append(f"{w}: unrecognized statement: {s[:60]}")

for p in sys.argv[1:]:
    lint(p)
print(f"relations: {len(arity)}")
for e in errors: print("ERROR  ", e)
for x in warnings: print("warn   ", x)
print("RESULT:", "FAIL" if errors else "PASS")
sys.exit(1 if errors else 0)
