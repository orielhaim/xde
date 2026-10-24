param([int]$Trials=3, [ValidateSet('a','e')][string[]]$Stages=@('e'),
    [ValidateSet('relaxed','periodic','crash-safe')][string[]]$Durability=@('relaxed','periodic'),
    [string]$OutputDirectory='target/pr1-checkpoint/trials',
    [Parameter(Mandatory)][string]$Baseline, [Parameter(Mandatory)][string]$Candidate)
$ErrorActionPreference='Stop'
. "$PSScriptRoot/../2026-10-20-scheduler/process.ps1"
foreach($policy in $Durability) {
    foreach($arm in @('baseline','candidate')) {New-Item -ItemType Directory -Force (Join-Path $OutputDirectory "$policy/$arm") | Out-Null}
}
$cases=@(foreach($policy in $Durability) {foreach($stage in $Stages) {@{Policy=$policy;Stage=$stage}}})
for($trial=0;$trial -lt $Trials;$trial++) {
    $ordered=@($cases); $arms=@('baseline','candidate')
    if($trial%2) {[array]::Reverse($ordered); [array]::Reverse($arms)}
    foreach($case in $ordered) {
        foreach($arm in $arms) {
            $path=Join-Path $OutputDirectory "$($case.Policy)/$arm/engine-$($case.Stage)-$trial.json"
            if(Test-Path -LiteralPath $path) {throw "Refusing to overwrite $path"}
            $exe=if($arm -eq 'baseline') {$Baseline} else {$Candidate}
            $r=Invoke-BenchmarkProcess $exe @('--protocol','h2','--connections','1','--streams','1','--pipeline',$case.Stage,'--size-mib','4096','--durability',$case.Policy,'--trials','1','--warmup','0') $path
            Write-Output "$($case.Policy) $($case.Stage) $trial $arm finished"
        }
    }
}
