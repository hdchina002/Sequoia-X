@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ==================================================
echo   Sequoia-X 策略预览（只看结果，不推送）
echo ==================================================
echo.
".venv\Scripts\python.exe" -u scripts\preview.py
echo.
echo ---------------- 预览结束 ----------------
pause
