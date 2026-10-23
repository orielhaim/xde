param([int]$Trials=3, [int]$SizeMiB=4096, [string]$OutputDirectory='target/pr1-receive/trials',
    [ValidateSet('h1','h2','h3')][string]$Protocol='h3', [switch]$EngineOnly,
    [string]$Engine='target/pr1-final/xde-bench.exe', [string]$ReceiveProbe='target/pr1-final/receive-probe.exe',
    [string[]]$CaseName=@())
$ErrorActionPreference='Stop'
. "$PSScriptRoot/../2026-10-20-scheduler/process.ps1"
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
$cases=@('quic-chunk','quic-batch','h3','fixture-h3','engine-a','engine-e')
if($EngineOnly) {$cases=@('engine-a','engine-e')}
elseif($Protocol -ne 'h3') {throw 'Raw QUIC/H3 references require H3; use EngineOnly for H1/H2'}
if($CaseName.Count) {
    $cases=@($cases | Where-Object {$_ -in $CaseName})
    if($cases.Count -ne $CaseName.Count) {throw 'Unknown or repeated case name'}
}
for ($trial=0; $trial -lt $Trials; $trial++) {
    $ordered=@($cases)
    if ($trial%2) {[array]::Reverse($ordered)}
    foreach ($case in $ordered) {
        $path=Join-Path $OutputDirectory "$case-$trial.json"
        if(Test-Path -LiteralPath $path) {throw "Refusing to overwrite $path"}
        if($case.StartsWith('engine-')) {
            $exe=$Engine
            $argv=@('--protocol',$Protocol,'--connections','1','--streams','1','--pipeline',$case.Substring(7),'--size-mib',"$SizeMiB",'--trials','1','--warmup','0')
        } else {
            $exe=$ReceiveProbe
            $argv=@('--mode',$case,'--size-mib',"$SizeMiB")
        }
        $r=Invoke-BenchmarkProcess $exe $argv $path
        Write-Output "$case $trial finished"
    }
}
