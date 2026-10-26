@echo off
rem Main experiments of pre-registration addendum 1 (commit c51caf0), run in order, detached from any shell.
rem Runs from the repository root whatever the current folder is. Python: %DVS_PYTHON% if set,
rem else .venv\Scripts\python.exe in the repository root.
cd /d "%~dp0.."
if not defined DVS_PYTHON set "DVS_PYTHON=.venv\Scripts\python.exe"
if not exist results\main mkdir results\main
set OMP_NUM_THREADS=1
set PYTHONIOENCODING=utf-8
echo START %DATE% %TIME% > results\main\RUN_MAIN.log
git rev-parse HEAD >> results\main\RUN_MAIN.log
"%DVS_PYTHON%" -m dvs.run --config configs\main_M1.yaml --workers 8 > results\main\M1.log 2>&1
echo M1 done %DATE% %TIME% exit %ERRORLEVEL% >> results\main\RUN_MAIN.log
"%DVS_PYTHON%" -m dvs.run --config configs\main_M2.yaml --workers 8 > results\main\M2.log 2>&1
echo M2 done %DATE% %TIME% exit %ERRORLEVEL% >> results\main\RUN_MAIN.log
"%DVS_PYTHON%" -m dvs.run --config configs\main_sens.yaml --workers 8 > results\main\sens.log 2>&1
echo sens done %DATE% %TIME% exit %ERRORLEVEL% >> results\main\RUN_MAIN.log
echo END %DATE% %TIME% >> results\main\RUN_MAIN.log
