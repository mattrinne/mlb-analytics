import os

# Initialize and define cleanup variables, specifying the number of days
#   counting back from today, within which the cleanup DAG will not perform deletion operations
CLEANUP_AIRFLOW_LOGS_RETENTION_DAYS = os.getenv('CLEANUP_AIRFLOW_LOGS_RETENTION_DAYS', 8)
CLEANUP_AIRFLOW_DB_RETENTION_DAYS = os.getenv('CLEANUP_AIRFLOW_DB_RETENTION_DAYS', 8)