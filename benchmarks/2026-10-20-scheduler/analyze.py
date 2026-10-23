"""Summarize every retained scheduler trial; CPU counters never select samples."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path


def distribution(values):
    ordered = sorted(values)
    def percentile(p):
        at = (len(ordered) - 1) * p
        lo = int(at)
        hi = min(lo + 1, len(ordered) - 1)
        return ordered[lo] + (ordered[hi] - ordered[lo]) * (at - lo)
    return dict(values=values, min=min(values), p10=percentile(.1),
                median=statistics.median(values), p90=percentile(.9), max=max(values))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    groups, records, identities, workloads = {}, [], {}, {}
    for path in sorted(args.directory.glob('*.json')):
        if not path.name.startswith(('Fixed-', 'Matched-', 'Adaptive-', 'Long-', 'Multi-', 'Scaling-')):
            continue
        report = json.loads(path.read_text(encoding='utf-8-sig'))
        section, rest = path.stem.split('-', 1)
        case, trial, arm = rest.rsplit('-', 2)
        identity = {key: report.get(key) for key in ('source_id', 'compiler', 'compio_revision',
                    'lock_blake3', 'build_target', 'build_profile', 'os', 'workload',
                    'jobs', 'shared_origin', 'weighted', 'queue', 'global_connections',
                    'global_assignments', 'quantum_bytes', 'mode')}
        cohort = (section, case, arm)
        assert identities.setdefault(cohort, identity) == identity, f'mixed cohort: {cohort}'
        assert workloads.setdefault((section, case), report['workload']) == report['workload'], 'mismatched paired workload'
        if section in ('Fixed', 'Matched', 'Scaling'):
            row = report['trials'][0]
            if 'Err' in row['xde']:
                records.append(dict(file=path.name, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                                    section=section, case=case, trial=int(trial), arm=arm,
                                    identity=identity, historical_validity=row['validity'],
                                    eligible=False, rejection_reason=row['xde']['Err']))
                continue
            payload = row['xde']['Ok']
            assert payload['artifact_verified_bytes'] == report['workload']['bytes']
            rate = payload['job_mib_s']
            load, transport = payload['load'], payload['transport']
            historical_status = row['validity']
        else:
            payload = report
            rate = report['aggregate_mib_s']
            load, transport = report['load'], report['transport']
            historical_status = None
        expected_bytes = report['workload']['bytes'] * report.get('jobs', 1)
        if section not in ('Fixed', 'Matched', 'Scaling'):
            assert report['published_bytes'] == expected_bytes
            assert report['engine_memory_used_after_jobs'] == 0
            if transport.get('peak_reserved_connections') is not None:
                assert transport['peak_reserved_connections'] <= report['global_connections']
        assert transport['response_validated_bytes'] == expected_bytes
        assert transport['assignments_started'] == transport['assignments_finished']
        assert not transport['missing_protocol_evidence']
        assert transport['payload_protocols'] == [{'h1':'http/1.1','h2':'h2','h3':'h3'}[report['workload']['protocol']]]
        if section in ('Fixed', 'Matched', 'Scaling'):
            if transport.get('peak_reserved_connections') is not None:
                assert transport['peak_reserved_connections'] <= report['workload']['connections']
            assert transport.get('peak_payload_connections', transport['payload_connections']) == report['workload']['connections']
            assert transport['peak_streams_per_connection'] == report['workload']['streams']
        record = dict(file=path.name, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                      eligible=True,
                      section=section, case=case, trial=int(trial), arm=arm,
                      job_mib_s=rate, source_id=report['source_id'], lock_blake3=report['lock_blake3'],
                      historical_validity=historical_status,
                      identity=identity,
                      protocols=transport['payload_protocols'], connections=transport['payload_connections'],
                      peak_connections=transport.get('peak_payload_connections', transport['payload_connections']),
                      peak_reserved_connections=transport.get('peak_reserved_connections'),
                      peak_streams=transport['peak_streams_per_connection'],
                      payload_mib_s=expected_bytes / 1048576 / transport['payload_to_sink_finish_seconds'],
                      fragments_per_mib=transport['receive_chunks'] / (expected_bytes / 1048576),
                      writes_per_mib=transport['destination_operations'] / (expected_bytes / 1048576),
                      memory_wait_seconds=transport['memory_wait_seconds'],
                      capacity_wait_seconds=transport['capacity_wait_seconds'],
                      batch_copy_seconds=transport['batch_copy_seconds'],
                      file_write_await_seconds=transport['file_write_await_seconds'],
                      cpu=load.get('during'), notes='All functionally matched trials retained; CPU diagnostic only.')
        if section not in ('Fixed', 'Matched', 'Scaling'):
            record.update(completion_seconds=report['completion_seconds'],
                          controller_percent=100 * report['controller_seconds'] / report['makespan_seconds'],
                          controller_observations=report['controller_observations'],
                          planner_evaluations=report['planner_evaluations'],
                          planner_bytes=report['planner_bytes_per_job'],
                          assignment_control_bytes=report['assignment_control_bytes'],
                          memory_after=report['engine_memory_used_after_jobs'])
            horizon = min(report['completion_offsets_seconds'])
            # Count only wholly consumed requests by the first completion. The final
            # overlapping request is deliberately omitted, never interpolated.
            service = {job: 0 for job in report['job_ids']}
            for request in transport['requests']:
                end = request.get('payload_finish_seconds')
                if end is not None and end <= horizon:
                    service[request['job']] += request['bytes']
            record['completed_request_bytes_at_first_completion'] = service
        records.append(record)
        groups.setdefault((section, case, arm), []).append((int(trial), rate))
    summaries = {'/'.join(k): distribution([rate for _, rate in sorted(v)]) for k, v in groups.items()}
    pairs = {}
    for section, case, arm in groups:
        left, right = ('baseline', 'current') if section in ('Fixed', 'Matched') else ('single', 'adaptive')
        if arm != left or (section, case, right) not in groups:
            continue
        if section == 'Matched':
            a_id, b_id = identities[(section, case, left)], identities[(section, case, right)]
            for field in ('compiler', 'compio_revision', 'lock_blake3', 'build_target', 'build_profile'):
                assert a_id[field] == b_id[field], f'mismatched comparison {field}'
        a, b = dict(groups[(section, case, left)]), dict(groups[(section, case, right)])
        assert a.keys() == b.keys(), 'incomplete pair cohort'
        pairs[f'{section}/{case}/{right}_over_{left}'] = distribution([b[i] / a[i] for i in sorted(a)])
    limitations = ['Three pairs are descriptive, not strong statistical evidence.',
                               'Buffered positional writes and verified publication, not durable device throughput.',
                               'No sample selected by CPU utilization or throughput.']
    if any(record.get('section') == 'Fixed' for record in records):
        limitations.append('The original Fixed reference uses a different lock hash; this comparison does not isolate scheduler changes.')
    result = dict(policy='workstation_v1', records=records, distributions=summaries, paired_ratios=pairs,
                  limitations=limitations)
    (args.directory / 'analysis.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(distributions=summaries, paired_ratios=pairs), indent=2))


if __name__ == '__main__':
    main()
