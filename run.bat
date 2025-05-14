@echo off
setlocal enabledelayedexpansion

echo Memeriksa instalasi Python...

where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Python belum terinstall. Menginstall Python 3.10.4...
    curl -o python-3.10.4-amd64.exe https://www.python.org/ftp/python/3.10.4/python-3.10.4-amd64.exe
    python-3.10.4-amd64.exe /quiet InstallAllUsers=1 PrependPath=1
    del python-3.10.4-amd64.exe
) else (
    for /f "tokens=2" %%i in ('python -c "import sys; print(sys.version.split()[0])"') do set PYTHON_VERSION=%%i
    echo Versi Python terinstall: !PYTHON_VERSION!
    
    for /f "tokens=1,2 delims=." %%a in ("!PYTHON_VERSION!") do (
        set MAJOR_VERSION=%%a
        set MINOR_VERSION=%%b
    )
    
    if not "!MAJOR_VERSION!"=="3" (
        set WRONG_VERSION=1
    ) else if not "!MINOR_VERSION!"=="10" (
        set WRONG_VERSION=1
    ) else (
        set WRONG_VERSION=0
    )
    
    if !WRONG_VERSION! equ 1 (
        echo Python 3.10 diperlukan, menghapus versi saat ini dan menginstall Python 3.10.4...
        
        for /f "tokens=2 delims=:" %%a in ('wmic product where "name like 'Python%%'" get name /value ^| find "="') do (
            echo Menghapus %%a...
            wmic product where "name='%%a'" call uninstall /nointeractive
        )
        
        echo Menginstall Python 3.10.4...
        curl -o python-3.10.4-amd64.exe https://www.python.org/ftp/python/3.10.4/python-3.10.4-amd64.exe
        python-3.10.4-amd64.exe /quiet InstallAllUsers=1 PrependPath=1
        del python-3.10.4-amd64.exe
    )
)

echo Memastikan pip tersedia...
python -m ensurepip --upgrade

echo Menginstall paket yang diperlukan dari requirements.txt...
pip install -r requirements.txt

echo Menjalankan aplikasi...
python main.py

endlocal
