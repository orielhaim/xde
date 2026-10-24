"""Paired fixed-topology H2 path isolation using the existing receive validator."""
import argparse
import importlib.util
import json
import sys
from pathlib import Path

PREVIOUS = Path(__file__).resolve().parent.parent / '2026-10-23-pr1'
sys.path.insert(0, str(PREVIOUS))
from analyze import distribution
spec = importlib.util.spec_from_file_location('receive_analysis', PREVIOUS / 'analyze-receive.py')
receive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receive)


def timeline(requests, job_seconds):
    spans = sorted((r['payload_start_seconds'], r['payload_finish_seconds'])
                   for r in requests if r['payload_start_seconds'] is not None
                   and r['payload_finish_seconds'] is not None)
    if not spans:
        return None
    end = spans[0][0]
    gap = 0.0
    for start, finish in spans:
        if finish < start:
            raise ValueError('reversed payload interval')
        gap += max(0.0, start - end)
        end = max(end, finish)
    return {'first_payload_seconds': spans[0][0],
            'payload_span_seconds': end - spans[0][0],
            'no_payload_interval_seconds': gap,
            'after_final_sink_seconds': job_seconds - end,
            'requests': len(requests),
            'request_mib': [r['bytes'] / (1 << 20) for r in requests],
            'summed_receive_seconds': sum(r['receive_seconds'] for r in requests),
            'summed_destination_seconds': sum(r['destination_seconds'] for r in requests),
            'write_poll': None if not all('write_first_ready' in r for r in requests) else {
                'first_ready': sum(r['write_first_ready'] for r in requests),
                'first_pending': sum(r['write_first_pending'] for r in requests),
                'first_poll_seconds': sum(r['write_first_poll_seconds'] for r in requests),
                'max_first_poll_seconds': max(r['write_max_first_poll_seconds'] for r in requests),
                'max_await_seconds': max(r['write_max_await_seconds'] for r in requests)}}


def analyze(directory, expected):
    arms = {}
    for name in ['baseline', 'candidate']:
        arms[name] = receive.analyze(directory / name)
        for record in arms[name]['records']:
            raw = json.loads((directory / name / record['file']).read_text(encoding='utf-8-sig'))
            trial = raw['trials'][0]
            if raw['source_id'] != expected[name]:
                record['reasons'].append('unexpected source identity')
            if trial['pipeline'].lower() != record['case'].removeprefix('engine-'):
                record['reasons'].append('pipeline mismatch')
            record['eligible'] = not record['reasons']
            record['identity'] = {k: raw[k] for k in ['workload', 'compiler', 'build_profile',
                                                    'build_target', 'os', 'compio_revision', 'lock_blake3']}
            if 'Ok' in trial['xde']:
                result = trial['xde']['Ok']
                record['timeline'] = timeline(result['transport']['requests'], result['job_seconds'])
    candidates = {(r['case'], r['trial']): r for r in arms['candidate']['records']}
    pairs, ratios = [], {}
    for a in arms['baseline']['records']:
        b = candidates.get((a['case'], a['trial']))
        if b is None:
            continue
        differences = {key: [a['identity'][key], b['identity'][key]] for key in a['identity']
                       if a['identity'][key] != b['identity'][key]}
        eligible = a['eligible'] and b['eligible'] and not differences
        pair = {'case': a['case'], 'trial': a['trial'], 'eligible': eligible,
                'identity_differences': differences, 'reasons': a['reasons'] + b['reasons']}
        if eligible:
            pair['ratio'] = b['metrics']['mib_s'] / a['metrics']['mib_s']
            ratios.setdefault(a['case'], []).append(pair['ratio'])
        pairs.append(pair)
    return {'schema_version': 1, 'arms': arms, 'pairs': pairs,
            'ratios': {key: distribution(values) for key, values in ratios.items()},
            'limitations': ['payload intervals include sink waiting and are not network activity intervals',
                            'summed concurrent waits overlap', 'no CPU rejection threshold']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--expected-sources', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.directory, json.loads(args.expected_sources.read_text(encoding='utf-8-sig')))
    (args.directory / 'analysis.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result['ratios'], indent=2))
