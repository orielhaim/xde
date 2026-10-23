param(
    [int]$Trials = 3,
    [int]$StartTrial = 0,
    [string[]]$CaseName = @(),
    [string]$OutputDirectory = 'target/pr1-control/pilot',
    [string]$Baseline = 'target/pr1-baseline/scheduler-probe.exe',
    [string]$Candidate = 'target/pr1-control/scheduler-probe.exe'
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/../2026-10-20-scheduler/process.ps1"
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
$cases = @(
    @{Name='h1-cold'; Args=@('--protocol','h1','--mode','adaptive','--size-mib','128','--jobs','1','--connections','4','--per-connection-mib-s','8')},
    @{Name='h2-fixed'; Args=@('--protocol','h2','--mode','single','--size-mib','4096','--jobs','1')},
    @{Name='h3-fixed'; Args=@('--protocol','h3','--mode','single','--size-mib','4096','--jobs','1')},
    @{Name='h1-8'; Args=@('--protocol','h1','--mode','single','--size-mib','8','--jobs','8','--global-connections','16','--assignments','16','--shared-origin')},
    @{Name='h1-128'; Args=@('--protocol','h1','--mode','single','--size-mib','8','--jobs','128','--global-connections','16','--assignments','16','--shared-origin')},
    @{Name='h2-adaptive'; Args=@('--protocol','h2','--mode','adaptive','--size-mib','4096','--jobs','1','--connections','4','--streams','4')},
    @{Name='h3-adaptive'; Args=@('--protocol','h3','--mode','adaptive','--size-mib','4096','--jobs','1','--connections','4','--streams','4')}
)
if ($CaseName.Count) {
    $cases = @($cases | Where-Object {$_.Name -in $CaseName})
    if ($cases.Count -ne $CaseName.Count) {throw 'Unknown or repeated case name'}
} else {
    $cases = @($cases | Where-Object {$_.Name -notin @('h2-adaptive','h3-adaptive')})
}
for ($trial=$StartTrial; $trial -lt $StartTrial + $Trials; $trial++) {
    $ordered = @($cases)
    if ($trial % 2) {[array]::Reverse($ordered)}
    foreach ($case in $ordered) {
        $arms = @('baseline','candidate')
        if ($trial % 2) {[array]::Reverse($arms)}
        foreach ($arm in $arms) {
            $path = Join-Path $OutputDirectory "$($case.Name)-$trial-$arm.json"
            if (Test-Path -LiteralPath $path) {throw "Refusing to overwrite $path"}
            $exe = if ($arm -eq 'baseline') {$Baseline} else {$Candidate}
            $r = Invoke-BenchmarkProcess $exe $case.Args $path
            Write-Output "$($case.Name) $trial $arm $($r.aggregate_mib_s) MiB/s"
        }
    }
}
