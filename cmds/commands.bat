@echo off
setlocal

SET data_path=data\landing

REM init docker (keep an eye on this run!!!! make sure to open docker and localhost:8080 on a browser when docker compose up -d finishes)
@echo on
docker compose down -v
docker compose up -d
docker compose ps
@echo off

REM wait for Airflow
echo.
echo Waiting for localhost:8080 to start.
:wait_for_airflow
curl -s -f -o nul http://localhost:8080
IF %ERRORLEVEL% NEQ 0 (
    timeout /t 5 >nul
    GOTO wait_for_airflow
)
echo Airflow webserver started.
timeout /t 3 >nul

REM first run - initial load
@echo on
docker compose exec airflow-scheduler airflow dags unpause sarisari_sales_etl
@echo off
echo Checking for any scheduled DAGs.
:wait_for_auto_run
docker compose exec airflow-scheduler airflow dags list-runs sarisari_sales_etl | findstr "scheduled queued running" >nul
IF %ERRORLEVEL% EQU 0 (
    echo No scheduled DAGs found.
) ELSE (
    echo No active runs found. Triggering manual run.
    @echo on
    docker compose exec airflow-scheduler airflow dags trigger sarisari_sales_etl
    @echo off
)
echo Waiting for the DAG run to finish.
:wait_for_dag
timeout /t 5 >nul
docker compose exec airflow-scheduler airflow dags list-runs sarisari_sales_etl --state running | findstr "running" >nul
IF %ERRORLEVEL% EQU 0 (
    GOTO wait_for_dag
)
echo DAG run finished.
@echo on
docker compose exec dwh-postgres psql -U dwh -d dwh -P pager=off -c "SELECT * FROM dw.dim_store;"
@echo off
REN "%data_path%\sales_2026-10.csv" "sales_2026-10.proc"

REM second run - eleven rows of new store
REN "%data_path%\sales_2026-10a.csv" "sales_2026-10.csv"
@echo on
docker compose exec airflow-scheduler airflow dags trigger sarisari_sales_etl
@echo off
echo Waiting for the DAG run to finish.
:wait_for_dag
timeout /t 5 >nul
docker compose exec airflow-scheduler airflow dags list-runs sarisari_sales_etl --state running | findstr "running" >nul
IF %ERRORLEVEL% EQU 0 (
    GOTO wait_for_dag
)
echo DAG run finished.
@echo on
docker compose exec dwh-postgres psql -U dwh -d dwh -P pager=off -c "SELECT * FROM dw.dim_store;"
@echo off
REN "%data_path%\sales_2026-10.csv" "sales_2026-10a.proc"

REM third run - one row of existing store with different location
REN "%data_path%\sales_2026-10b.csv" "sales_2026-10.csv"
@echo on
docker compose exec airflow-scheduler airflow dags trigger sarisari_sales_etl
@echo off
echo Waiting for the DAG run to finish.
:wait_for_dag
timeout /t 5 >nul
docker compose exec airflow-scheduler airflow dags list-runs sarisari_sales_etl --state running | findstr "running" >nul
IF %ERRORLEVEL% EQU 0 (
    GOTO wait_for_dag
)
echo DAG run finished.
@echo on
docker compose exec dwh-postgres psql -U dwh -d dwh -P pager=off -c "SELECT * FROM dw.dim_store;"
@echo off
REN "%data_path%\sales_2026-10.csv" "sales_2026-10b.proc"

REM cleanup
FOR %%F IN (%data_path%\*.proc) DO (
    REN "%%F" "%%~nF.csv"
)
@echo on
docker compose exec airflow-scheduler airflow dags list-runs sarisari_sales_etl