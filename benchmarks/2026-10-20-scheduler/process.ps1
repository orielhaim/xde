function Invoke-BenchmarkProcess {
    param([string]$Executable, [string[]]$Arguments, [string]$OutputPath, [int]$TimeoutSeconds = 180)
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
