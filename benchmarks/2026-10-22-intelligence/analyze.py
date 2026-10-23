"""Keep every matched, functionally correct trial; CPU is diagnostic metadata."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path


def distribution(values):
    s=sorted(values)
    def percentile(p):
        n=(len(s)-1)*p
        i=int(n)
        return s[i]+(s[min(i+1,len(s)-1)]-s[i])*(n-i)
    return dict(values=values,min=min(s),p10=percentile(.1),median=statistics.median(s),p90=percentile(.9),max=max(s))


def parallel_time(requests, target):
    events=[]
    for r in requests:
        if r.get('payload_start_seconds') is not None and r.get('payload_finish_seconds') is not None:
            events.extend([(r['payload_start_seconds'],1,r['connection']),(r['payload_finish_seconds'],-1,r['connection'])])
    active={}
    for at,change,connection in sorted(events):
        active[connection]=active.get(connection,0)+change
        if sum(n>0 for n in active.values())>=target:
            return at
    return None


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    args=parser.parse_args()
    records=[]
    groups={}
    builds={}
    workloads={}
    for path in sorted(args.directory.glob('*.json')):
        if path.stem=='analysis':continue
        case,trial,arm=path.stem.rsplit('-',2)
        if arm not in ('baseline','current','fixed'):continue
        r=json.loads(path.read_text(encoding='utf-8-sig'))
        if 'diagnostic_stop' in r:
            records.append(dict(file=path.name,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),status='not_run',reason=r['diagnostic_stop']))
            continue
        identity=tuple(r[k] for k in ('source_id','compiler','compio_revision','lock_blake3','build_target','build_profile'))
        assert builds.setdefault(arm,identity)==identity, 'mixed source/build cohort'
        workload={k:r[k] for k in ('workload','jobs','shared_origin','weighted','queue','global_connections','global_assignments','quantum_bytes','shared_egress_mib_s','source_mib_s')}
        assert workloads.setdefault(case,workload)==workload, 'mismatched comparison'
        t=r['transport']
        expected=r['jobs']*r['workload']['bytes']
        assert r['published_bytes']==t['response_validated_bytes']==expected
        assert r['engine_memory_used_after_jobs']==0
        assert t['assignments_started']==t['assignments_finished']
        assert not t['missing_protocol_evidence']
        assert t['payload_protocols']==[{'h1':'http/1.1','h2':'h2','h3':'h3'}[r['workload']['protocol']]]
        assert t['peak_reserved_connections']<=r['global_connections']
        assert t['peak_streams_per_connection']<=r['workload']['streams']
        records.append(dict(file=path.name,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            case=case,trial=int(trial),arm=arm,identity=identity,functional_eligible=True,
            aggregate_mib_s=r['aggregate_mib_s'],completion_seconds=r['completion_seconds'],
            payload_mib_s=expected/1048576/t['payload_to_sink_finish_seconds'],
            first_two_payload_connections=parallel_time(t['requests'],2),
            first_four_payload_connections=parallel_time(t['requests'],4),
            peak_connections=t['peak_payload_connections'],peak_reserved=t['peak_reserved_connections'],peak_streams=t['peak_streams_per_connection'],
            fragments_per_mib=t['receive_chunks']/(expected/1048576),operations_per_mib=t['destination_operations']/(expected/1048576),
            controller_percent=100*r['controller_seconds']/r['makespan_seconds'],controller_observations=r['controller_observations'],
            planner_evaluations=r['planner_evaluations'],topology_changes=r['planner_topology_changes'],
            planner_bytes=r['planner_bytes_per_job'],assignment_control_bytes=r['assignment_control_bytes'],cpu=r['load']['during']))
        groups.setdefault((case,arm),[]).append((int(trial),r['aggregate_mib_s']))
    for arm in ('current','fixed'):
        if arm in builds and 'baseline' in builds:assert builds[arm][1:]==builds['baseline'][1:], 'unmatched dependencies/compiler'
    summaries={'/'.join(key):distribution([v for _,v in sorted(values)]) for key,values in groups.items()}
    pairs={}
    for case,arm in groups:
        if arm=='baseline':continue
        a=dict(groups[(case,'baseline')]); b=dict(groups[(case,arm)])
        assert a.keys()==b.keys(), 'incomplete pair'
        pairs[f'{case}/{arm}_over_baseline']=distribution([b[i]/a[i] for i in sorted(a)])
    result=dict(schema_version=1,validity_policy='workstation_v1',records=records,distributions=summaries,paired_ratios=pairs,
        limitations=['Three trials are descriptive, not confidence intervals.','Shared egress is bounded-burst application pacing, not network RTT/congestion emulation.','No throughput or CPU-based sample selection.'])
    (args.directory/'analysis.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(trials=len(records),paired_ratios=pairs),indent=2))


if __name__=='__main__':main()
