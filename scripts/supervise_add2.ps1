# Memory-aware supervisor for a resumable run queue (used for the Addendum 2 runs).
# Once a minute it stops at an END line in the queue's run log or at a Traceback in any .log file next
# to it, restarts RuntimeBroker if it holds more than 6 GB, kills the queue when free commit memory falls
# below 2 GB, and restarts the queue with 1 to 3 workers when at least 14 GB is free. The queue must call
# scripts\run_missing.py with "--workers N", so finished runs are skipped on restart. The supervisor
# rewrites the worker count inside the queue file before each start.
# The Addendum 2 queue used locally is not included; scripts\run_add3.cmd has the same shape:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\supervise_add2.ps1 -Queue scripts\run_add3.cmd -RunLog results\addendum3\RUN_ADD3.log
# Relative paths are taken from the repository root.
param(
  [Parameter(Mandatory = $true)][string]$Queue,
  [string]$RunLog = 'results\main\RUN_ADD2.log',
  [string]$SupervisorLog = 'scripts\supervisor.log'
)
Set-Location (Split-Path -Parent $PSScriptRoot)
$f = (Resolve-Path $Queue).Path; $r2 = Split-Path -Parent $RunLog; $runlogname = Split-Path -Leaf $RunLog; $logf = $SupervisorLog
$qname = Split-Path -Leaf $f
function Running { @(Get-CimInstance Win32_Process -Filter "Name='cmd.exe'" | Where-Object { $_.CommandLine -like "*$qname*" }) }
while($true){
  $log=Get-Content "$r2\$runlogname" -Raw -ErrorAction SilentlyContinue
  if($log -match '(?m)^END'){ "$(Get-Date -f s) END seen" | Add-Content $logf; break }
  if((Get-ChildItem "$r2\*.log" -ErrorAction SilentlyContinue | Select-String -Pattern 'Traceback' | Measure-Object).Count -gt 0){ "$(Get-Date -f s) Traceback seen, stopping supervisor" | Add-Content $logf; break }
  Get-Process RuntimeBroker -ErrorAction SilentlyContinue | Where-Object { $_.PrivateMemorySize64 -gt 6GB } | ForEach-Object { Stop-Process -Id $_.Id -Force -Confirm:$false; "$(Get-Date -f s) restarted RuntimeBroker" | Add-Content $logf }
  $free=(Get-CimInstance Win32_OperatingSystem).FreeVirtualMemory/1MB
  $run=Running
  if($run.Count -gt 0 -and $free -lt 2){ foreach($x in $run){ taskkill /PID $x.ProcessId /T /F | Out-Null }; "$(Get-Date -f s) paused, free=$([math]::Round($free,1))" | Add-Content $logf }
  elseif($run.Count -eq 0 -and $free -ge 14){ $w= if($free -ge 30){3} elseif($free -ge 22){2} else {1}; (Get-Content $f) -replace '--workers \d','--workers '+$w -replace 'workers \d >>','workers '+$w+' >>' | Set-Content $f -Encoding ascii; Start-Process -FilePath 'cmd.exe' -ArgumentList '/c',$f -WindowStyle Hidden; "$(Get-Date -f s) started with $w workers, free=$([math]::Round($free,1))" | Add-Content $logf }
  Start-Sleep 60
}
