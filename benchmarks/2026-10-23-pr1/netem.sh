#!/usr/bin/env bash
set -euo pipefail
# All interface changes occur in a new namespace, including failure paths.
exec unshare --net -- python3 - "${1:-/mnt/d/Projects/xde/target/pr1-netem/trials}" "${2:-3}" <<'PY'
import json, os, platform, signal, subprocess, sys
from pathlib import Path

output = Path(sys.argv[1]).resolve()
trials = int(sys.argv[2])
if trials < 1:
    raise ValueError('Trials must be positive')
if output.exists() and any(output.iterdir()):
    raise ValueError('Use a new output directory to retain previous observations')
output.mkdir(parents=True, exist_ok=True)
metadata = output / 'metadata'
metadata.mkdir()
base = Path('/home/oriel/.cache/xde-pr1')
executables = {arm: base / 'coherent' / arm / 'scheduler-probe'
               for arm in ('baseline', 'candidate')}
for executable in executables.values():
    if not executable.is_file():
        raise FileNotFoundError(executable)
def run(*args):
    return subprocess.check_output(args, text=True)
run('ip', 'link', 'set', 'lo', 'up')
run('tc', 'qdisc', 'add', 'dev', 'lo', 'root', 'netem', 'delay', '10ms', 'limit', '10000')
ping = run('ping', '-q', '-c', '5', '127.0.0.1')
if '5 received' not in ping:
    raise RuntimeError('Network-delay pilot failed its ping check')
(metadata / 'ping.txt').write_text(ping)
(metadata / 'environment.json').write_text(json.dumps({'kernel': platform.release(), 'namespace': os.readlink('/proc/self/ns/net'), 'configured_delay_ms': 10, 'configured_loss_percent': 0, 'network_emulation': True, 'process_cpu_scope': 'GNU time whole process, including fixture'}, indent=2))
cases = [
    ('h1-rtt20', ['--protocol','h1','--mode','adaptive','--size-mib','128','--jobs','1','--connections','4','--per-connection-mib-s','8']),
    ('h2-rtt20', ['--protocol','h2','--mode','single','--size-mib','128','--jobs','1']),
    ('h3-rtt20', ['--protocol','h3','--mode','single','--size-mib','128','--jobs','1'])
]
for trial in range(trials):
    for case, args in (cases if trial % 2 == 0 else reversed(cases)):
        for arm in (['baseline','candidate'] if trial % 2 == 0 else ['candidate','baseline']):
            name = f'{case}-{trial}-{arm}'
            (metadata / f'{name}-before.json').write_text(run('tc','-s','-j','qdisc','show','dev','lo'))
            command = ['/usr/bin/time','-v','-o',str(metadata/f'{name}.time.txt'), str(executables[arm]), *args]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
            try:
                stdout, stderr = process.communicate(timeout=180)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
            (output / f'{name}.json').write_text(stdout)
            (metadata / f'{name}.stderr').write_text(stderr)
            (metadata / f'{name}-after.json').write_text(run('tc','-s','-j','qdisc','show','dev','lo'))
            if process.returncode:
                raise RuntimeError(f'{name} failed with {process.returncode}; raw output retained')
            row = json.loads(stdout)
            if row['engine_memory_used_after_jobs'] or row['published_bytes'] != row['jobs'] * row['workload']['bytes']:
                raise RuntimeError(f'{name} failed publication/memory checks')
            print(name, row['aggregate_mib_s'], 'MiB/s', flush=True)
PY
