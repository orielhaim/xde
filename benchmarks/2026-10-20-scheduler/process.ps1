function Invoke-BenchmarkProcess {
    param([string]$Executable, [string[]]$Arguments, [string]$OutputPath, [int]$TimeoutSeconds = 180, [long]$ProcessorAffinity = 0)
    $info = [System.Diagnostics.ProcessStartInfo]::new((Resolve-Path -LiteralPath $Executable).Path)
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $info.ArgumentList.Add($argument) }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $info
    try {
        $null = $process.Start()
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if ($ProcessorAffinity) {
            try {
                $process.ProcessorAffinity = [IntPtr]::new($ProcessorAffinity)
                $actual = $process.ProcessorAffinity.ToInt64()
                if ($actual -ne $ProcessorAffinity) {throw 'Affinity readback mismatch'}
                @{pid=$process.Id;requested_affinity=$ProcessorAffinity;actual_affinity=$actual;priority_class="$($process.PriorityClass)";diagnostic_only=$true;applied_utc=[DateTime]::UtcNow.ToString('o')} |
                    ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath "$OutputPath.affinity"
            } catch {
                $process.Kill($true)
                $process.WaitForExit()
                throw
            }
        }
        $finished = $process.WaitForExit($TimeoutSeconds * 1000)
        if (!$finished) { $process.Kill($true); $process.WaitForExit() }
        $stdout.GetAwaiter().GetResult() | Set-Content -LiteralPath $OutputPath -Encoding utf8
        $errorText = $stderr.GetAwaiter().GetResult()
        if ($errorText.Trim().Length -gt 0) { $errorText | Set-Content -LiteralPath "$OutputPath.stderr" -Encoding utf8 }
        if (!$finished) { throw "benchmark timed out; raw output: $OutputPath" }
        if ($process.ExitCode -ne 0) { throw "benchmark failed ($($process.ExitCode)); raw output: $OutputPath" }
        return Get-Content -LiteralPath $OutputPath -Raw | ConvertFrom-Json
    } finally { $process.Dispose() }
}
