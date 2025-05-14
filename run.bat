@echo off
setlocal enabledelayedexpansion

echo Memeriksa instalasi Python 3.10...

:: Cek apakah Python 3.10 sudah terinstall
python --version 2>nul | findstr /r "3\.10\." >nul
if %errorlevel% equ 0 (
    echo Python 3.10 sudah terinstall.
    set PYTHON_CMD=python
) else (
    :: Cek apakah py launcher dapat menemukan Python 3.10
    py -3.10 --version 2>nul | findstr /r "3\.10\." >nul
    if %errorlevel% equ 0 (
        echo Python 3.10 ditemukan menggunakan py launcher.
        set PYTHON_CMD=py -3.10
    ) else (
        echo Python 3.10 tidak ditemukan.
        echo Mengunduh dan menginstal Python 3.10.4...
        
        :: Unduh Python 3.10.4 installer
        curl -L -o python-3.10.4.exe https://www.python.org/ftp/python/3.10.4/python-3.10.4-amd64.exe
        
        if not exist python-3.10.4.exe (
            echo Gagal mengunduh Python 3.10.4. Silakan instal secara manual.
            pause
            exit /b 1
        )
        
        :: Instal Python 3.10.4 (dengan menambahkan ke PATH, dan pip)
        echo Menginstal Python 3.10.4...
        python-3.10.4.exe /quiet InstallAllUsers=1 PrependPath=1 Include_pip=1
        
        :: Hapus installer setelah instalasi
        del python-3.10.4.exe
        
        :: Tentukan command yang akan digunakan
        set PYTHON_CMD=python
        
        :: Verifikasi instalasi
        %PYTHON_CMD% --version 2>nul | findstr /r "3\.10\." >nul
        if %errorlevel% neq 0 (
            echo Instalasi Python 3.10.4 gagal. Silakan instal secara manual.
            pause
            exit /b 1
        )
    )
)

echo Menggunakan: %PYTHON_CMD%
echo.

:: Cek apakah requirements.txt ada
if exist requirements.txt (
    echo Menginstal paket-paket yang diperlukan dari requirements.txt...
    %PYTHON_CMD% -m pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo Gagal menginstal paket-paket. Silakan coba lagi.
        pause
        exit /b 1
    )
) else (
    echo File requirements.txt tidak ditemukan.
)

:: Cek apakah main.py ada
if exist main.py (
    echo Menjalankan main.py...
    %PYTHON_CMD% main.py
) else (
    echo File main.py tidak ditemukan.
)

pause
