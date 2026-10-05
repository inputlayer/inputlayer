#!/bin/bash
# Per-binary timing table from a `cargo test` log: each test binary's run
# time ("finished in" of its result line) and test count, slowest first,
# with the total. A binary that crashed before its result line shows no time.
#
# Usage: ./scripts/test-timings.sh <cargo-test.log>   (markdown on stdout)

LOG="${1:?usage: $0 <cargo-test.log>}"

sed 's/\x1b\[[0-9;]*m//g' "$LOG" | awk '
    /^ *Running / {
        # "Running tests/foo.rs (target/debug/deps/foo-0123abcd)"
        name = $2
        if (name == "unittests") name = $3
        if (match($0, /\(.*\)/)) {
            bin = substr($0, RSTART + 1, RLENGTH - 2)
            sub(/.*\//, "", bin)
            sub(/-[0-9a-f]+$/, "", bin)
            name = name " (" bin ")"
        }
        current = name
        next
    }
    /^ *Doc-tests / { current = "doc-tests " $2; next }
    /^test result: / && current != "" {
        passed = $4; failed = $6
        secs = $NF; sub(/s$/, "", secs)
        printf "%s\t%d\t%d\t%s\n", secs, passed, failed, current
        current = ""
    }
' | sort -t$'\t' -k1,1gr | awk -F'\t' '
    BEGIN {
        print "| Binary | Tests | Failed | Seconds |"
        print "|---|---:|---:|---:|"
    }
    {
        printf "| %s | %d | %d | %.2f |\n", $4, $2, $3, $1
        total += $1; tests += $2; failed += $3; n++
    }
    END { printf "| **total (%d binaries)** | %d | %d | %.2f |\n", n, tests, failed, total }
'
