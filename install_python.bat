@echo off
echo Installing Python 3.10.4...

:: Run PowerShell commands
powershell -Command "Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.10.4/python-3.10.4-amd64.exe' -OutFile 'python-installer.exe'"
powershell -Command "Start-Process -FilePath 'python-installer.exe' -ArgumentList '/quiet InstallAllUsers=1 PrependPath=1' -Wait"

:: Clean up installer
del python-installer.exe

echo Python installation completed!
pause 