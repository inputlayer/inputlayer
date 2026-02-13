#!/usr/bin/env python3
"""Migrate documentation from old Datalog syntax to new InputLayer syntax.

Old syntax:  :- , ?- , % comments, .dl, trailing dots on statements
New syntax:  <- , ? , // comments, .idl, no trailing dots

Usage:
  python3 scripts/migrate_docs_syntax.py --dry-run     # Preview changes
  python3 scripts/migrate_docs_syntax.py                # Apply changes
  python3 scripts/migrate_docs_syntax.py --file <path>  # Single file
"""

import re
import sys
import os
from pathlib import Path


def is_datalog_code_block(fence_lang: str) -> bool:
    """Check if a code fence language is a Datalog-like block."""
    return fence_lang.strip().lower() in ("datalog", "prolog", "")


def migrate_code_line(line: str, in_ebnf: bool) -> str:
    """Migrate a single line inside a code block from old to new syntax."""
    if in_ebnf:
        # EBNF blocks: replace literal strings like ":-" → "<-" and "?-" → "?"
        line = line.replace('":-"', '"<-"')
        line = line.replace('"?-"', '"?"')
        return line

    # Don't touch lines that are purely comments (already migrated or meta commands)
    stripped = line.strip()
    if stripped.startswith("//"):
        # But still migrate .dl → .idl in comments
        return re.sub(r'\.dl\b', '.idl', line)
    if stripped.startswith("."):
        # Meta commands - update .dl references and % comments
        line = re.sub(r'\.dl\b', '.idl', line)
        # Also convert % comments on meta command lines
        code_with_spacing, comment_part = split_code_and_comment(line)
        if comment_part is not None:
            code_trimmed = code_with_spacing.rstrip()
            trailing_ws = code_with_spacing[len(code_trimmed):]
            comment_part = re.sub(r'\.dl\b', '.idl', comment_part)
            return code_trimmed + trailing_ws + '//' + comment_part
        return line

    # Split the line into code part and comment part first.
    # This allows us to process trailing dots correctly on the code portion.
    code_with_spacing, comment_part = split_code_and_comment(line)

    # Separate trailing whitespace from code
    code_trimmed = code_with_spacing.rstrip()
    trailing_ws = code_with_spacing[len(code_trimmed):]

    # Replace :- with <- (but not inside strings)
    code_trimmed = replace_outside_strings(code_trimmed, ' :- ', ' <- ')
    code_trimmed = replace_outside_strings(code_trimmed, '\t:- ', '\t<- ')
    if code_trimmed.rstrip().endswith(':-'):
        code_trimmed = code_trimmed.rstrip()[:-2] + '<-'
    code_trimmed = replace_outside_strings(code_trimmed, '):-', ')<-')
    code_trimmed = replace_outside_strings(code_trimmed, ') :-', ') <-')

    # Replace ?- with ? for queries (eat the space after ?- too)
    code_lstripped = code_trimmed.lstrip()
    if code_lstripped.startswith('?- '):
        leading = code_trimmed[:len(code_trimmed) - len(code_lstripped)]
        code_trimmed = leading + '?' + code_lstripped[3:]
    elif code_lstripped.startswith('?-'):
        leading = code_trimmed[:len(code_trimmed) - len(code_lstripped)]
        code_trimmed = leading + '?' + code_lstripped[2:]

    # Remove trailing dot from statements
    code_trimmed = remove_trailing_dot(code_trimmed)

    # Replace .dl file references
    code_trimmed = re.sub(r'\.dl\b', '.idl', code_trimmed)

    # Reassemble with comment (now using //)
    if comment_part is not None:
        comment_part = re.sub(r'\.dl\b', '.idl', comment_part)
        return code_trimmed + trailing_ws + '//' + comment_part
    else:
        return code_trimmed


def split_code_and_comment(line: str) -> tuple:
    """Split a line into (code_part, comment_text_or_None).

    The comment text includes everything after the % sign (but not the % itself).
    The code_part preserves spacing so that when reassembled as code_part + '//' + comment,
    the spacing between code and comment is preserved.
    Returns (line, None) if there's no comment.
    Handles % as modulo operator (inside rule bodies after :- or with surrounding math).
    """
    in_string = False
    i = 0
    while i < len(line):
        c = line[i]
        if c == '"' and not in_string:
            in_string = True
        elif c == '"' and in_string:
            in_string = False
        elif c == '\\' and in_string and i + 1 < len(line):
            i += 1  # skip escaped char
        elif c == '%' and not in_string:
            before = line[:i].rstrip()
            # Check if this % is a modulo operator
            # Modulo appears as `expr % operand` where operand is a number or short variable
            # followed by comma, paren, or comparison. Comments have descriptive text.
            if before and (before[-1].isalnum() or before[-1] in ')_'):
                rest_after = line[i + 1:]
                rest_stripped = rest_after.lstrip()
                # Modulo: followed by a space then a number or short variable,
                # then a delimiter (comma, paren, comparison, space, end of line)
                # e.g., "X % 2," or "A % B)" or "N % 10."
                # NOT a comment like "% Decimal" or "% This is a comment"
                modulo_match = re.match(
                    r'\s*(\d+|[A-Z][a-zA-Z0-9]?)\s*[,).><=!;\s]',
                    rest_after + ' '
                )
                if modulo_match:
                    # Check it's not followed by English text (comments)
                    first_word = rest_stripped.split()[0] if rest_stripped.split() else ''
                    if first_word and (first_word[0].isdigit() or
                            (len(first_word) <= 2 and first_word[0].isupper())):
                        i += 1
                        continue
            # This is a comment
            # Keep the whitespace between code and comment marker
            code_with_trailing = line[:i]
            comment_text = line[i + 1:]
            return (code_with_trailing, comment_text)
        i += 1
    return (line, None)


def replace_outside_strings(line: str, old: str, new: str) -> str:
    """Replace `old` with `new` but only outside of string literals."""
    result = []
    in_string = False
    i = 0
    while i < len(line):
        if line[i] == '"' and (i == 0 or line[i - 1] != '\\'):
            in_string = not in_string
            result.append(line[i])
            i += 1
        elif not in_string and line[i:i + len(old)] == old:
            result.append(new)
            i += len(old)
        else:
            result.append(line[i])
            i += 1
    return ''.join(result)


def remove_trailing_dot(line: str) -> str:
    """Remove trailing dot from Datalog statements.

    Preserves dots in:
    - Meta commands (.kg, .rel, .rule, .session, .load, etc.)
    - Decimal numbers (3.14)
    - Strings
    - Empty lines
    - Lines ending with ... (ellipsis)
    """
    stripped = line.rstrip()
    if not stripped:
        return line
    if not stripped.endswith('.'):
        return line
    # Don't touch meta commands
    if stripped.lstrip().startswith('.'):
        return line
    # Don't remove if it's an ellipsis (...)
    if stripped.endswith('...'):
        return line
    # Don't remove dot if it's part of a decimal number like "3.14)"
    # But DO remove it from "25." at end (that's integer + trailing dot)
    # A decimal dot has a digit on BOTH sides: digit.digit
    if len(stripped) >= 2 and stripped[-2].isdigit():
        # Check if this is truly a decimal: is there also a digit after the dot?
        # At end of line, there's nothing after - so it's a trailing statement dot
        # Exception: if the char before the last digit-dot is also part of a float like "40.7"
        # We only keep the dot if it's inside a float literal that's NOT at the very end
        # e.g., "+location(40.7, -74.0)." - the dot after ) is trailing, dots in 40.7 are decimal
        # Since we already stripped to the last char being '.', and [-2] is a digit,
        # this IS likely a trailing dot after a number: +edge(1, 2). or +result(25).
        # The only exception would be a bare float ending the line: but "3.14." isn't valid
        pass  # fall through to remove
    # Remove the trailing dot, preserving any trailing whitespace after the dot
    trailing = line[len(stripped):]
    return stripped[:-1] + trailing


def migrate_prose_line(line: str) -> str:
    """Migrate prose (non-code) lines."""
    # Replace .dl file extension references (but not in URLs or paths that are already .idl)
    # Match patterns like: .dl extension, .dl file, .dl format, *.dl, file.dl
    line = re.sub(r'`\.dl`', '`.idl`', line)
    line = re.sub(r'\.dl\b(?!\.)(?!l)', '.idl', line)

    # Replace inline code with old syntax
    # `?- body.` → `?body`
    # `:- ` → `<- ` in inline code
    # We process inline backtick spans
    line = migrate_inline_code(line)

    return line


def migrate_inline_code(line: str) -> str:
    """Process inline backtick code spans to migrate syntax."""
    result = []
    i = 0
    while i < len(line):
        if line[i] == '`':
            # Find closing backtick
            end = line.find('`', i + 1)
            if end == -1:
                result.append(line[i:])
                break
            code = line[i + 1:end]
            # Migrate the code span
            code = code.replace(' :- ', ' <- ')
            code = code.replace('):- ', ') <- ')
            code = code.replace(':-', '<-')  # catch-all
            if code.startswith('?-'):
                code = '?' + code[2:]
            # Remove trailing dot from statement-like code
            if code.endswith('.') and not code.startswith('.') and not re.search(r'\d\.$', code):
                code = code[:-1]
            # .dl → .idl
            code = re.sub(r'\.dl\b', '.idl', code)
            result.append('`')
            result.append(code)
            result.append('`')
            i = end + 1
        else:
            result.append(line[i])
            i += 1
    return ''.join(result)


def migrate_file(filepath: str, dry_run: bool = False) -> tuple:
    """Migrate a single file. Returns (changes_count, lines_changed)."""
    with open(filepath, 'r') as f:
        original = f.read()

    lines = original.split('\n')
    new_lines = []
    changes = 0
    in_code_block = False
    code_fence_lang = ""
    in_ebnf = False

    for line in lines:
        # Detect code fence boundaries
        stripped = line.strip()
        if stripped.startswith('```'):
            if in_code_block:
                # Closing fence
                in_code_block = False
                in_ebnf = False
                new_lines.append(line)
                continue
            else:
                # Opening fence
                in_code_block = True
                fence_lang = stripped[3:].strip()
                code_fence_lang = fence_lang
                in_ebnf = fence_lang.lower() == 'ebnf'

                # Change ```prolog to ```datalog
                if fence_lang.lower() == 'prolog':
                    line = line.replace('prolog', 'datalog')
                    changes += 1

                new_lines.append(line)
                continue

        if in_code_block:
            if is_datalog_code_block(code_fence_lang) or in_ebnf:
                new_line = migrate_code_line(line, in_ebnf)
            else:
                new_line = line
        else:
            new_line = migrate_prose_line(line)

        if new_line != line:
            changes += 1
        new_lines.append(new_line)

    result = '\n'.join(new_lines)

    if result != original:
        if not dry_run:
            with open(filepath, 'w') as f:
                f.write(result)
        return (changes, True)
    return (0, False)


def find_doc_files(base_dirs: list) -> list:
    """Find all .md files in the given directories."""
    files = []
    for base_dir in base_dirs:
        base = Path(base_dir)
        if base.is_file() and base.suffix == '.md':
            files.append(str(base))
        elif base.is_dir():
            for md_file in sorted(base.rglob('*.md')):
                # Skip node_modules, target, .git
                parts = md_file.parts
                if any(p in ('node_modules', 'target', '.git', '.claude') for p in parts):
                    continue
                files.append(str(md_file))
    return files


def main():
    dry_run = '--dry-run' in sys.argv
    single_file = None

    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == '--file' and i + 1 < len(args):
            single_file = args[i + 1]

    # Determine project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    course_dir = os.path.dirname(project_dir)

    if single_file:
        files = [single_file]
    else:
        # Process all doc directories
        dirs = [
            os.path.join(project_dir, 'docs'),
            os.path.join(project_dir, 'README.md'),
            os.path.join(project_dir, 'TESTING.md'),
            os.path.join(course_dir, 'CLAUDE.md'),
            os.path.join(course_dir, 'README.md'),
            os.path.join(course_dir, 'QUICKSTART.md'),
            os.path.join(course_dir, 'QUICK_START_GUIDE.md'),
            os.path.join(course_dir, 'ARCHITECTURE.md'),
            os.path.join(course_dir, 'SOLUTIONS_REFERENCE.md'),
            os.path.join(course_dir, 'COURSE_OVERVIEW.md'),
            os.path.join(course_dir, 'INDEX.md'),
            os.path.join(course_dir, 'TODO.md'),
        ]
        files = find_doc_files(dirs)

    total_changes = 0
    files_changed = 0

    mode = "DRY RUN" if dry_run else "APPLYING"
    print(f"[{mode}] Processing {len(files)} file(s)...\n")

    for filepath in files:
        if not os.path.exists(filepath):
            continue
        changes, changed = migrate_file(filepath, dry_run)
        if changed:
            rel_path = os.path.relpath(filepath, course_dir)
            print(f"  {'WOULD CHANGE' if dry_run else 'CHANGED'}: {rel_path} ({changes} replacements)")
            total_changes += changes
            files_changed += 1

    print(f"\nTotal: {total_changes} replacements in {files_changed} file(s)")
    if dry_run:
        print("(Dry run - no files modified. Remove --dry-run to apply.)")


if __name__ == '__main__':
    main()
