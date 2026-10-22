param(
    [ValidateSet('Pilot','Matrix')][string]$Section='Pilot',
    [int]$Trials=3,
    [string[]]$CaseName=@(),
    [string]$OutputDirectory='target/intelligence/measured',
    [string]$Baseline='target/intelligence/baseline/final-scheduler-probe.exe',
    [string]$Current='target/release/scheduler-probe.exe'
)
$ErrorActionPreference='Stop'
. "$PSScriptRoot/../2026-10-20-scheduler/process.ps1"
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
$cases = if ($Section -eq 'Pilot') { @(
    @{Name='h1-cold'; Protocol='h1'; Size=128; Jobs=1; Extra=@('--per-connection-mib-s','8')},
    @{Name='h3-shared'; Protocol='h3'; Size=64; Jobs=2; Extra=@('--shared-egress-mib-s','16')}
) } else { @(
    @{Name='h1-small'; Protocol='h1'; Size=1; Jobs=1; Extra=@()},
    @{Name='h1-long'; Protocol='h1'; Size=512; Jobs=1; Extra=@('--per-connection-mib-s','8')},
    @{Name='h2-loopback'; Protocol='h2'; Size=4096; Jobs=1; Extra=@()},
    @{Name='h3-loopback'; Protocol='h3'; Size=4096; Jobs=1; Extra=@()},
    @{Name='h2-shared'; Protocol='h2'; Size=128; Jobs=2; Extra=@('--shared-egress-mib-s','32')},
    @{Name='h1-heterogeneous'; Protocol='h1'; Size=128; Jobs=2; Extra=@('--per-connection-mib-s','8','--source-mib-s','8,64','--global-connections','3')},
    @{Name='h2-heterogeneous'; Protocol='h2'; Size=64; Jobs=2; Extra=@('--source-mib-s','8,32','--global-connections','3')},
    @{Name='h3-heterogeneous'; Protocol='h3'; Size=64; Jobs=2; Extra=@('--source-mib-s','8,32','--global-connections','3')}
) }
if($CaseName.Count) {
    $cases=@($cases | Where-Object {$_.Name -in $CaseName})
    if($cases.Count -ne $CaseName.Count) {throw 'unknown or repeated case name'}
}
for($trial=0;$trial -lt $Trials;$trial++) {
    $ordered=@($cases)
    if($trial%2 -eq 1) {[array]::Reverse($ordered)}
    foreach($case in $ordered) {
        $arms=@('baseline','current','fixed')
        if($trial%2 -eq 1) {[array]::Reverse($arms)}
        foreach($arm in $arms) {
            $path=Join-Path $OutputDirectory "$($case.Name)-$trial-$arm.json"
            if(Test-Path -LiteralPath $path) {throw "refusing to overwrite $path"}
            $exe=if($arm -eq 'baseline') {$Baseline} else {$Current}
            $mode=if($arm -eq 'fixed') {if($case.Protocol -eq 'h1' -and $case.Name -ne 'h1-small') {'fixed'} else {'single'}} else {'adaptive'}
            $streams=if($case.Protocol -eq 'h1') {'1'} else {'4'}
            $argv=@('--protocol',$case.Protocol,'--mode',$mode,'--size-mib',"$($case.Size)",'--jobs',"$($case.Jobs)",'--connections','4','--streams',$streams)+$case.Extra
            $r=Invoke-BenchmarkProcess $exe $argv $path
            if($r.published_bytes -ne $case.Size*$case.Jobs*1MB -or $r.engine_memory_used_after_jobs -ne 0) {throw "artifact/budget failure $path"}
            Write-Output "$($case.Name) $trial $arm $($r.aggregate_mib_s) MiB/s completion=$($r.completion_seconds -join ',') changes=$($r.planner_topology_changes)"
        }
    }
}
