## Summary

<!-- What changed and why. Link the issue it closes or contributes to. -->

## Verification

<!-- Targeted tests, `make test-fast`, affected `.iql.out` specs, adversarial cases. -->

## Pre-PR gate

<!-- `make pre-pr` must pass before opening the PR (CONTRIBUTING). It runs the
     fast checks in parallel, then the performance gate (perf-gate/README.md). -->

- `make pre-pr`: <!-- pass / which step failed -->
- Hot path: <!-- none (test/docs/tooling only) | path touched and expected cost -->
- `make perf-gate` verdict: <!-- PASS / FAIL / INCONCLUSIVE / INVALID, baseline commit, host -->

<details><summary>Performance gate report</summary>

<!-- Paste target/perf-gate/latest/report.md -->

</details>
