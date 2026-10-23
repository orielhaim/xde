param([int]$Trials=3, [int]$SizeMiB=1024,
    [string]$Executable='target/pr1-coherent/candidate/scheduler-probe.exe',
    [string]$OutputDirectory='target/pr1-coherent/fixture-scaling')
$ErrorActionPreference='Stop'
. "$PSScriptRoot/../2026-10-20-scheduler/process.ps1"
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
for ($trial=0; $trial -lt $Trials; $trial++) {
    $cases=@('shared','independent')
    if($trial%2) {[array]::Reverse($cases)}
    foreach($case in $cases) {
        $path=Join-Path $OutputDirectory "$case-$trial.json"
        if(Test-Path -LiteralPath $path) {throw "Refusing to overwrite $path"}
        $argv=@('--protocol','h3','--mode','single','--size-mib',"$SizeMiB",
            '--jobs','2','--connections','1','--streams','1',
            '--global-connections','2','--assignments','2')
        if($case -eq 'shared') {$argv+='--shared-origin'}
        $r=Invoke-BenchmarkProcess $Executable $argv $path
        if($r.published_bytes -ne 2*$SizeMiB*1MB -or $r.engine_memory_used_after_jobs -ne 0) {
            throw "Publication/memory check failed: $path"
        }
        Write-Output "$case $trial $($r.aggregate_mib_s) MiB/s"
    }
}
