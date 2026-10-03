import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
STUB = '''#!/usr/bin/env python3
import json, os, sys, time
name = os.path.basename(sys.argv[0])
args = sys.argv[1:]
with open('events.jsonl', 'a') as f:
    f.write(json.dumps([name, args, 'start', time.monotonic()]) + '\\n')
time.sleep(0.05)
with open('events.jsonl', 'a') as f:
    f.write(json.dumps([name, args, 'end', time.monotonic()]) + '\\n')
if os.environ.get('FAIL_CHECK') == name:
    sys.exit(1)
'''


class PrePrRoutingTests(unittest.TestCase):
    def run_gate(self, paths, fail=None):
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            repo = Path(tmp)
            shutil.copy(ROOT / 'Makefile', repo / 'Makefile')
            for path in ('packages/inputlayer-py', 'packages/inputlayer-js', 'perf-gate', 'src', 'scripts', 'bin'):
                (repo / path).mkdir(parents=True, exist_ok=True)
            for name in ('cargo', 'uv', 'npm'):
                path = repo / 'bin' / name
                path.write_text(STUB)
                path.chmod(0o755)
            for name in ('test-affected.sh', 'perf-gate.sh'):
                path = repo / 'scripts' / name
                path.write_text(STUB)
                path.chmod(0o755)
            env = dict(os.environ, PATH=f"{repo / 'bin'}:{os.environ['PATH']}")
            env.pop('MAKEFLAGS', None)
            if fail:
                env['FAIL_CHECK'] = fail
            for args in (['init', '-q'], ['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'fixture']):
                subprocess.run(['git', *args], cwd=repo, env=env, check=True, capture_output=True)
            for path in paths:
                (repo / path).write_text('changed\n')
            subprocess.run(['git', 'add', '.'], cwd=repo, env=env, check=True, capture_output=True)
            result = subprocess.run(['make', 'pre-pr', 'PRE_PR_BASE=HEAD'], cwd=repo, env=env, capture_output=True, text=True, timeout=20)
            events = []
            for event_file in repo.rglob('events.jsonl'):
                events.extend(json.loads(line) for line in event_file.read_text().splitlines())
            return result, events

    def test_routes_each_component_and_orders_performance_last(self):
        cases = [
            ([], set()),
            (['src/change.rs'], {'test', 'clippy', 'test-affected.sh'}),
            (['packages/inputlayer-py/change.py'], {'uv'}),
            (['packages/inputlayer-js/change.ts'], {'npm'}),
            (['perf-gate/change.rs'], {'test', 'clippy'}),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                result, events = self.run_gate(paths)
                self.assertEqual(result.returncode, 0, result.stderr)
                starts = [event for event in events if event[2] == 'start']
                checks = {args[0] if name == 'cargo' else name for name, args, _, _ in starts}
                self.assertEqual(checks, expected | {'fmt', 'perf-gate.sh'})
                if 'npm' in expected:
                    self.assertEqual({tuple(e[1]) for e in starts if e[0] == 'npm'}, {('ci', '--ignore-scripts'), ('test',), ('run', 'typecheck')})
                perf_start = next(e[3] for e in starts if e[0] == 'perf-gate.sh')
                self.assertTrue(all(e[3] < perf_start for e in events if e[0] != 'perf-gate.sh'))

    def test_component_checks_overlap(self):
        result, events = self.run_gate(['packages/inputlayer-py/change.py', 'packages/inputlayer-js/change.ts', 'perf-gate/change.rs'])
        self.assertEqual(result.returncode, 0, result.stderr)
        starts = [e[3] for e in events if e[2] == 'start' and e[0] != 'perf-gate.sh']
        first_end = min(e[3] for e in events if e[2] == 'end')
        self.assertGreater(sum(t < first_end for t in starts), 1)

    def test_failure_prevents_performance_run(self):
        result, events = self.run_gate(['packages/inputlayer-js/change.ts'], fail='npm')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(e[0] == 'perf-gate.sh' for e in events))


if __name__ == '__main__':
    unittest.main()
