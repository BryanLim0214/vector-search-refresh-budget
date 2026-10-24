@echo off
rem Main experiments of pre-registration addendum 2, run in order, detached from any shell.
rem Refuse to start unless E:\DistributedDatabaseProject\repo\results\main\RUN_MAIN.log contains a line starting with END.
if not exist "E:\DistributedDatabaseProject\repo\results\main\RUN_MAIN.log" (
    echo ERROR: E:\DistributedDatabaseProject\repo\results\main\RUN_MAIN.log does not exist. Refusing to start before main queue finishes.
    exit /b 1
)
findstr /b /c:"END" "E:\DistributedDatabaseProject\repo\results\main\RUN_MAIN.log" >nul 2>&1
if errorlevel 1 (
    echo ERROR: E:\DistributedDatabaseProject\repo\results\main\RUN_MAIN.log does not contain a line starting with END. Refusing to start while repo queue is running.
    exit /b 1
)
cd /d E:\DistributedDatabaseProject\repo-prereg2
if not exist results\main mkdir results\main
set OMP_NUM_THREADS=1
set MKL_NUM_THREADS=1
set OPENBLAS_NUM_THREADS=1
set NUMEXPR_NUM_THREADS=1
set PYTHONIOENCODING=utf-8
set WORKERS=3
echo START %DATE% %TIME% > results\main\RUN_ADD2.log
git rev-parse HEAD >> results\main\RUN_ADD2.log
E:\DistributedDatabaseProject\repo\.venv\Scripts\python.exe -m dvs.run --config configs\main_M1_online.yaml --workers %WORKERS% > results\main\M1_online.log 2>&1
echo M1_online done %DATE% %TIME% exit %ERRORLEVEL% >> results\main\RUN_ADD2.log
E:\DistributedDatabaseProject\repo\.venv\Scripts\python.exe -m dvs.run --config configs\main_M3.yaml --workers %WORKERS% > results\main\M3.log 2>&1
echo M3 done %DATE% %TIME% exit %ERRORLEVEL% >> results\main\RUN_ADD2.log
echo END %DATE% %TIME% >> results\main\RUN_ADD2.log
rem After all runs finish, build combined analysis root and run analyze.py:
rem E:\DistributedDatabaseProject\repo\.venv\Scripts\python.exe scripts\link_results.py --analysis-root E:\DistributedDatabaseProject\analysis_root
rem E:\DistributedDatabaseProject\repo\.venv\Scripts\python.exe analysis\analyze.py --results-root E:\DistributedDatabaseProject\analysis_root --out E:\DistributedDatabaseProject\analysis_root\main
