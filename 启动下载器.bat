@echo off
cd /d "%~dp0"
where pythonw.exe >nul 2>nul
if not errorlevel 1 (
    start "" pythonw.exe "%~dp0aria2_gui.py"
    exit /b
)
if exist "%USERPROFILE%\miniconda3\pythonw.exe" (
    start "" "%USERPROFILE%\miniconda3\pythonw.exe" "%~dp0aria2_gui.py"
    exit /b
)
where py.exe >nul 2>nul
if not errorlevel 1 (
    start "" py.exe -3 "%~dp0aria2_gui.py"
    exit /b
)
echo Python 3 is required. Please install Python or activate your conda environment.
pause
