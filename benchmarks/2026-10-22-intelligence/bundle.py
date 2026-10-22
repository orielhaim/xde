"""Bundle source provenance and every measurement cohort without binaries."""
import hashlib,json,zipfile
from pathlib import Path
root=Path(__file__).resolve().parents[2]
base=root/'target/intelligence'
cohorts={'final':'diagnostic: timer-granularity fixture and early policy','accepted':'superseded policy: first-claim grants fixed','stable':'superseded policy: shared-link arms race','joint':'superseded policy: small-request sizing defect, includes explicit not-run sentinel','final-quality':'superseded policy: H2 sizing pilot before selected cold prior','selected':'primary selected policy, all completed matched trials','bulk-diagnostic':'single-pair diagnostic, not statistical evidence'}
files={}
for name in cohorts:
 for p in (base/name).glob('*'):
  if p.is_file() and p.suffix in ['.json','.log']:files[str(p.relative_to(base))]=p
for p in base.glob('*.log'):files[p.name]=p
for name in ['baseline','selected-binaries']:
 for file in ['source.zip','hash.json','binaries.json']:
  p=base/name/file
  if p.exists():files[str(p.relative_to(base))]=p
for p in (base/'baseline').glob('*'):
 if p.is_file() and p.suffix=='.json':files[str(p.relative_to(base))]=p
p=base/'selected-tables.md'
if p.exists():files[p.name]=p
manifest={name:hashlib.sha256(p.read_bytes()).hexdigest() for name,p in sorted(files.items())}
output=root/'benchmarks/2026-10-22-intelligence/raw.zip'
with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
 for name,p in sorted(files.items()):z.write(p,name)
 z.writestr('sha256.json',json.dumps(manifest,indent=2)+'\n')
 z.writestr('cohorts.json',json.dumps(cohorts,indent=2)+'\n')
print(len(files),'files',output.stat().st_size,'bytes')
