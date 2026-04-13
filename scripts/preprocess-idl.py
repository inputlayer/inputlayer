#!/usr/bin/env python3
"""Preprocess an .iql file: strip comments and join multi-line statements.

Multi-line rules (lines ending with '<-' or ',') are joined with
the following line. Output is one statement per line, ready for
the inputlayer-client --script flag.

Usage: python3 preprocess-idl.py <input.iql>
"""
import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: preprocess-idl.py <input.iql>", file=sys.stderr)
        sys.exit(1)

    lines = open(sys.argv[1]).readlines()
    stmt = ""
    for line in lines:
        # Strip comments (// to end of line)
        s = line.split("//")[0].strip()
        if not s:
            # Blank line = statement boundary
            if stmt.strip():
                print(stmt.strip())
            stmt = ""
            continue
        # If this line starts a new statement and we have a complete
        # previous statement (not ending with <- or ,), flush it
        if (
            (s.startswith("+") or s.startswith("?") or s.startswith("."))
            and stmt.strip()
            and not stmt.rstrip().endswith("<-")
            and not stmt.rstrip().endswith(",")
        ):
            print(stmt.strip())
            stmt = ""
        stmt += " " + s

    if stmt.strip():
        print(stmt.strip())

if __name__ == "__main__":
    main()
