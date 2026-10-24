@echo off
rem Main experiments of pre-registration addendum 2, run in order, detached from any shell.
cd /d E:\DistributedDatabaseProject\repo-prereg2
if not exist results\main mkdir results\main
set OMP_NUM_THREADS=1
set MKL_NUM_THREADS=1
set OPENBLAS_NUM_THREADS=1
set NUMEXPR_NUM_THREADS=1
set PYTHONIOENCODING=utf-8
echo START %DATE% %TIME% > results\main\RUN_ADD2.log
git rev-parse HEAD >> results\main\RUN_ADD2.log
E:\DistributedDatabaseProject\repo\.venv\Scripts\python.exe -m dvs.run --config configs\main_M1_online.yaml --workers 8 > results\main\M1_online.log 2>&1
echo M1_online done %DATE% %TIME% exit %ERRORLEVEL% >> results\main\RUN_ADD2.log
E:\DistributedDatabaseProject\repo\.venv\Scripts\python.exe -m dvs.run --config configs\main_M3.yaml --workers 8 > results\main\M3.log 2>&1
echo M3 done %DATE% %TIME% exit %ERRORLEVEL% >> results\main\RUN_ADD2.log
echo END %DATE% %TIME% >> results\main\RUN_ADD2.log
