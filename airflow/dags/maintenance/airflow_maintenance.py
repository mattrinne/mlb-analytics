import subprocess
import logging
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
# from airflow.operators.python_operator import PythonOperator
from common.constants import (
    CLEANUP_AIRFLOW_LOGS_RETENTION_DAYS,
    CLEANUP_AIRFLOW_DB_RETENTION_DAYS,
)

log = logging.getLogger(__name__)

# Define the DAG for cleaning up Airflow logs and database
@dag(
    'airflow_cleanup_dag',
    description='DAG for cleaning up Airflow logs and database',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule="0 11 * * *",
    catchup=False,
    tags=["maintenance"]
)

def airflow_maintenance(dry_run: bool = False):
    @task(task_id='cleanup_logs')
    def cleanup_logs():
        """
        Cleans up Airflow logs based on the specified retention period.

        This function identifies and deletes log files older than a specified number of days.
        It supports a dry run mode where the files to be deleted are only listed and not actually removed.

        Args:
            **conf (dict): Configuration passed from the DAG, including 'dry_run_logs' flag.
        """
        dry_run_logs = dry_run
        logs_path = 'logs'
        # Command to find log files older than the specified number of days
        find_command = f"find {logs_path} -name '*.log' -type f -mtime +{CLEANUP_AIRFLOW_LOGS_RETENTION_DAYS}"

        # Log the mode and path for clarity
        logging.info(f"Dry run mode: {dry_run_logs}")
        logging.info(f"Logs path: {logs_path}")

        # Modify the command for dry run or actual deletion
        if dry_run_logs:
            find_command += " -print"
        else:
            find_command += " -delete"

        # Execute the command and log the output
        logging.info(f"Cleanup command: {find_command}")
        result = subprocess.run(find_command, shell=True, capture_output=True, text=True)
        logging.info("Log files that would be deleted:")
        logging.info(result.stdout)

        # Only proceed to delete empty directories if it's not a dry run
        if not dry_run_logs:
            # Find and delete empty directories
            find_dirs_command = f"find {logs_path} -type d -empty -delete"
            subprocess.run(find_dirs_command, shell=True, check=True)
            logging.info("Empty directories have been deleted.")

    cleanup_the_logs = cleanup_logs()

    @task(task_id='cleanup_db')
    def cleanup_db():
        """
        Cleans up Airflow database entries based on the specified retention period.

        This function purges database records older than a specified number of days.
        It supports a dry run mode where the actions to be taken are only simulated.

        Args:
            **conf (dict): Configuration passed from the DAG, including 'dry_run_db' flag.
        """
        dry_run_db = dry_run
        cutoff_date = (datetime.now() - timedelta(days=CLEANUP_AIRFLOW_DB_RETENTION_DAYS)).strftime(
            '%Y-%m-%d %H:%M:%S')
        db_clean_command = f"airflow db clean --verbose --yes --clean-before-timestamp '{cutoff_date}'"

        # Log the mode and cutoff date for clarity
        logging.info(f"Dry run mode: {dry_run_db}")
        logging.info(f"Cutoff date: {cutoff_date}")

        # Modify the command for dry run or actual execution
        if dry_run_db:
            db_clean_command += " --dry-run"
        logging.info(f"Executing command: {db_clean_command}")
        result = subprocess.run(db_clean_command, shell=True, check=True)

        # Log the result or error of the command
        if result.returncode != 0:
            logging.error("Error occurred while cleaning the DB:")
            logging.error(result.stderr)
        else:
            logging.info("DB cleanup command executed successfully.")
            logging.info(result.stdout)
    
    cleanup_the_db = cleanup_db()


    # Set task dependencies
    cleanup_the_logs >> cleanup_the_db

airflow_maintenance()