"""Render the complete selected cohort; never select trials by throughput or CPU."""
import json
from collections import defaultdict
from pathlib import Path
import statistics

root=Path(__file__).resolve().parents[2]
a=json.loads((root/'target/intelligence/selected/analysis.json').read_text(encoding='utf-8'))
records=a['records']
assert len(records)==90, 'expected ten workloads, three arms and three trials'
groups=defaultdict(list)
for r in records:groups[(r['case'],r['arm'])].append(r)
lines=['## Selected matched results','','Aggregate useful MiB/s; all three observations are shown in trial order. Ratios are paired current/baseline throughput, not ratios of independent medians.','','| Workload | Previous adaptive | Selected adaptive | Fixed reference | Paired ratios |','| --- | --- | --- | --- | --- |']
fmt=lambda xs:', '.join(f'{x:.3f}' for x in xs)
for case in sorted({c for c,_ in groups}):
 values={arm:a['distributions'][f'{case}/{arm}']['values'] for arm in ['baseline','current','fixed']}
 ratios=a['paired_ratios'][f'{case}/current_over_baseline']
 lines.append('| '+case+' | '+' | '.join(fmt(values[arm]) for arm in ['baseline','current','fixed'])+' | '+fmt(ratios['values'])+' |')
lines+=['','Full p10/median/p90/min/max, individual completion times, payload intervals, CPU attribution and request records are in `selected/analysis.json` and raw JSON. Three observations are not enough to distinguish small changes confidently.','','## Observed control cost','','| Workload | Controller handling % (all selected trials) | Planner evaluations | Topology changes |','| --- | --- | --- | --- |']
for case in sorted({c for c,_ in groups}):
 rs=sorted(groups[(case,'current')],key=lambda x:x['trial'])
 lines.append(f'| {case} | '+fmt([r['controller_percent'] for r in rs])+' | '+', '.join(str(r['planner_evaluations']) for r in rs)+' | '+', '.join(str(r['topology_changes']) for r in rs)+' |')
selected=next(r for r in records if r['arm']=='current')
lines+=['',f"Planner state: {selected['planner_bytes']} bytes/job; assignment control: {selected['assignment_control_bytes']} bytes/assignment. These are structure sizes, not total job/transport RSS."]
(root/'target/intelligence/selected-tables.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
print('wrote complete tables')
