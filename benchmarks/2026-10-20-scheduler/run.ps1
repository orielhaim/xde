param(
    [ValidateSet('Fixed', 'Matched', 'Adaptive', 'Long', 'Multi', 'Scaling')][string]$Section = 'Fixed',
    [string]$OutputDirectory = 'target/reassessment/scheduler-measurements',
    [ValidateRange(1,1000)][int]$Trials = 3,
    [string]$CurrentExecutable = 'target/release/xde-bench.exe',
    [string]$SchedulerExecutable = 'target/release/scheduler-probe.exe',
    [string]$BaselineExecutable = 'target/reassessment/scheduler-baseline/xde-bench.exe',
    [string]$MatchedBaselineExecutable = 'target/reassessment/scheduler-baseline/matched-lock-xde-bench.exe'
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/process.ps1"
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$baseline = $BaselineExecutable
$current = $CurrentExecutable
$scheduler = $SchedulerExecutable
$cases = switch ($Section) {
    Fixed { @(
        @{Name='h1'; Protocol='h1'; Streams=1},
        @{Name='h2'; Protocol='h2'; Streams=1},
        @{Name='h3'; Protocol='h3'; Streams=1},
        @{Name='h3-4s'; Protocol='h3'; Streams=4}
    ) }
    Matched { @(
        @{Name='h1'; Protocol='h1'; Streams=1},
        @{Name='h2'; Protocol='h2'; Streams=1}
    ) }
    Adaptive { @(
        @{Name='capped-h1'; Protocol='h1'; Size=128; Extra=@('--per-connection-mib-s','8'); Streams=1},
        @{Name='h3'; Protocol='h3'; Size=4096; Extra=@(); Streams=4}
    ) }
    Long { @(
        @{Name='capped-h1'; Protocol='h1'; Size=512; Extra=@('--per-connection-mib-s','8'); Streams=1}
    ) }
    Multi { @(
        @{Name='fair-shared'; Extra=@('--shared-origin','--global-connections','1','--assignments','1')},
        @{Name='weighted-shared'; Extra=@('--shared-origin','--weighted','--global-connections','1','--assignments','1')},
        @{Name='fair-independent'; Extra=@('--global-connections','2','--assignments','2')},
        @{Name='queue-independent'; Extra=@('--queue','1','--global-connections','2','--assignments','2')}
    ) }
    Scaling { @(
        @{Name='h1-2c'; Protocol='h1'; Connections=2; Streams=1},
        @{Name='h1-4c'; Protocol='h1'; Connections=4; Streams=1},
        @{Name='h1-8c'; Protocol='h1'; Connections=8; Streams=1},
        @{Name='h2-2s'; Protocol='h2'; Connections=1; Streams=2},
        @{Name='h2-4s'; Protocol='h2'; Connections=1; Streams=4},
        @{Name='h2-8s'; Protocol='h2'; Connections=1; Streams=8},
        @{Name='h2-2c'; Protocol='h2'; Connections=2; Streams=1},
        @{Name='h2-4c'; Protocol='h2'; Connections=4; Streams=1},
        @{Name='h3-2s'; Protocol='h3'; Connections=1; Streams=2},
        @{Name='h3-8s'; Protocol='h3'; Connections=1; Streams=8},
        @{Name='h3-2c'; Protocol='h3'; Connections=2; Streams=1},
        @{Name='h3-4c'; Protocol='h3'; Connections=4; Streams=1}
    ) }
}
for ($trial=0; $trial -lt $Trials; $trial++) {
    $ordered = @($cases)
    if ($trial % 2 -eq 1) { [array]::Reverse($ordered) }
    foreach ($case in $ordered) {
        $arms = if ($Section -in @('Fixed','Matched')) { @('baseline','current') } elseif ($Section -in @('Adaptive','Long')) { @('single','adaptive') } else { @('fixed') }
        if ($trial % 2 -eq 1) { [array]::Reverse($arms) }
        foreach ($arm in $arms) {
            $path = Join-Path $OutputDirectory "$Section-$($case.Name)-$trial-$arm.json"
            if (Test-Path -LiteralPath $path) { throw "refusing to overwrite $path" }
            if ($Section -in @('Fixed','Matched')) {
                $exe = if ($arm -eq 'baseline') { if ($Section -eq 'Matched') { $MatchedBaselineExecutable } else { $baseline } } else { $current }
                $arguments = @('--protocol',$case.Protocol,'--connections','1','--streams',"$($case.Streams)",'--size-mib','4096','--trials','1','--warmup','0')
            } elseif ($Section -eq 'Scaling') {
                $exe = $current
                $arguments = @('--protocol',$case.Protocol,'--connections',"$($case.Connections)",'--streams',"$($case.Streams)",'--size-mib','4096','--trials','1','--warmup','0')
            } elseif ($Section -in @('Adaptive','Long')) {
                $exe = $scheduler
                $arguments = @('--protocol',$case.Protocol,'--mode',$arm,'--jobs','1','--connections','4','--streams',"$($case.Streams)",'--size-mib',"$($case.Size)") + $case.Extra
            } else {
                $exe = $scheduler
                $arguments = @('--protocol','h1','--mode','single','--jobs','2','--connections','1','--streams','1','--size-mib','64','--per-connection-mib-s','8') + $case.Extra
            }
            $report = Invoke-BenchmarkProcess $exe $arguments $path
            if ($Section -in @('Fixed','Matched','Scaling')) {
                $row = $report.trials[0]
                if (!$row.xde.Ok) { throw "functional failure: $path" }
                $payload = $row.xde.Ok
                if ($payload.artifact_verified_bytes -ne 4294967296 -or $payload.transport.response_validated_bytes -ne 4294967296) { throw "publication/telemetry mismatch: $path" }
                Write-Output "$Section $($case.Name) $trial $arm $($payload.job_mib_s) MiB/s historical_status=$($row.validity.status)"
            } else {
                Write-Output "$Section $($case.Name) $trial $arm $($report.aggregate_mib_s) MiB/s"
            }
        }
    }
}

