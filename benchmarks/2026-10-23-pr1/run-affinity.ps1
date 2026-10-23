param(
    [int]$Trials=3,
    [string]$OutputDirectory='target/pr1-affinity/trials',
    [string]$Baseline='target/pr1-matched-baseline/scheduler-probe.exe',
    [string]$Candidate='target/pr1-final/scheduler-probe.exe',
    [int[]]$EfficiencyClass=@()
)
$ErrorActionPreference='Stop'
. "$PSScriptRoot/../2026-10-20-scheduler/process.ps1"
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
$cpu=Get-Content -Raw target/pr1-final/cpu-sets.json | ConvertFrom-Json
if(@($cpu.cpu_sets | Where-Object {$_.group -ne 0 -or $_.logical_index -ge 63}).Count) {throw 'Pilot supports one processor group only'}
$classes=@($cpu.cpu_sets | Group-Object efficiency_class | Sort-Object Name)
if($EfficiencyClass.Count) {
    $classes=@($classes | Where-Object {[int]$_.Name -in $EfficiencyClass})
    if($classes.Count -ne $EfficiencyClass.Count) {throw 'Unknown or repeated CPU efficiency class'}
}
for($trial=0;$trial -lt $Trials;$trial++) {
    $ordered=@($classes)
    if($trial%2) {[array]::Reverse($ordered)}
    foreach($class in $ordered) {
        [long]$mask=0
        foreach($processor in $class.Group) {$mask=$mask -bor ([long]1 -shl $processor.logical_index)}
        $arms=@('baseline','candidate')
        if($trial%2) {[array]::Reverse($arms)}
        foreach($arm in $arms) {
            $path=Join-Path $OutputDirectory "h2-class-$($class.Name)-$trial-$arm.json"
            if(Test-Path -LiteralPath $path) {throw "Refusing to overwrite $path"}
            $exe=if($arm -eq 'baseline') {$Baseline} else {$Candidate}
            $r=Invoke-BenchmarkProcess $exe @('--protocol','h2','--mode','single','--size-mib','4096','--jobs','1') $path -ProcessorAffinity $mask
            Write-Output "class $($class.Name) trial $trial $arm $($r.aggregate_mib_s) MiB/s"
        }
    }
}
