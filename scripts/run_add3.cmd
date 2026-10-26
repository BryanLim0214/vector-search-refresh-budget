@echo off
rem Addendum 3 runs (exploratory, branch prereg2, committed before this script runs). Replications first.
rem Runs from the repository root whatever the current folder is. Python: %DVS_PYTHON% if set,
rem else .venv\Scripts\python.exe in the repository root.
cd /d "%~dp0.."
if not defined DVS_PYTHON set "DVS_PYTHON=.venv\Scripts\python.exe"
if not exist results\addendum3 mkdir results\addendum3
set OMP_NUM_THREADS=1
set MKL_NUM_THREADS=1
set OPENBLAS_NUM_THREADS=1
set NUMEXPR_NUM_THREADS=1
set PYTHONIOENCODING=utf-8
echo START %DATE% %TIME% workers 3 >> results\addendum3\RUN_ADD3.log
git rev-parse HEAD >> results\addendum3\RUN_ADD3.log
"%DVS_PYTHON%" scripts\run_missing.py --config configs\add3_replication.yaml --workers 1 > results\addendum3\replication.log 2>&1
echo replication done %DATE% %TIME% exit %ERRORLEVEL% >> results\addendum3\RUN_ADD3.log
"%DVS_PYTHON%" scripts\run_missing.py --config configs\add3_M1_replication.yaml --workers 1 > results\addendum3\M1_replication.log 2>&1
echo M1 replication done %DATE% %TIME% exit %ERRORLEVEL% >> results\addendum3\RUN_ADD3.log
"%DVS_PYTHON%" scripts\run_missing.py --config configs\add3_M3_dc_charged.yaml --workers 3 > results\addendum3\M3_dc_charged.log 2>&1
echo M3 charged done %DATE% %TIME% exit %ERRORLEVEL% >> results\addendum3\RUN_ADD3.log
"%DVS_PYTHON%" scripts\run_missing.py --config configs\add3_M1_dc_charged.yaml --workers 3 > results\addendum3\M1_dc_charged.log 2>&1
echo M1 charged done %DATE% %TIME% exit %ERRORLEVEL% >> results\addendum3\RUN_ADD3.log
echo END %DATE% %TIME% >> results\addendum3\RUN_ADD3.log
