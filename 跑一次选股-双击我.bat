@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ==================================================
echo   Sequoia-X 手动跑一次（策略计算 + 飞书推送）
echo   加 --force：忽略“今天已推送过”的判断
echo ==================================================
echo.
".venv\Scripts\python.exe" -u scripts\daily_run.py --force
echo.
echo ---------------- 运行结束 ----------------
pause
