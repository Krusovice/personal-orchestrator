from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# === Config ===
LOCAL_FOLDER = "/home/pi/user/secrets-and-database"
REMOTE_FOLDER = "gdrive:personal-webpage-backups/secrets-and-database"
LOG_FILE = "/home/pi/logs/gdrive_backup.log"

with DAG(
    dag_id="personal_webpage_weekly_backup",
    start_date=datetime(2026, 1, 20),
    schedule_interval="0 3 * * 0",  # Every Sunday at 03:00
    catchup=False,
    max_active_runs=1,
    tags=["backup", "gdrive"],
) as dag:

    backup_task = BashOperator(
        task_id="sync_folder_to_gdrive",
        bash_command=f"""
        rclone sync "{LOCAL_FOLDER}" "{REMOTE_FOLDER}" \
            --progress \
            --log-level INFO \
            --log-file "{LOG_FILE}" \
            --stats 10s
        """
    )
