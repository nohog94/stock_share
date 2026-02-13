@echo off
echo Stopping back_test.py...
powershell -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*back_test.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host 'Terminated process ' $_.ProcessId }"
echo Done.
pause
