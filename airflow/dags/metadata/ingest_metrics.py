import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)


@dag(
    'ingest_mlb_metrics',
    description='Get the latest position data from the mlb "metrics" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_metrics():
    @task(task_id='get_metrics_data')
    def get_metrics_data():
        """
        Get the latest data from the mlb "metrics" endpoint.
        """
        log.info('Calling mlb "metrics" endpoint"')

        metrics = mlb.get('meta', {'type': "metrics"})

        data = []
        for metric in metrics:
            if metric['name'] != "":
                group = metric['group'] if 'group' in metric else None
                unit = metric['unit'] if 'unit' in metric else None
                data.append((metric['metricId'], metric['name'], group, unit))

        return data
    
    @task(task_id='insert_metric_data')
    def insert_metric_data(data: list):
        """
        Insert the data into the "metric" table.
        """
        log.info('Inserting data into the "metrics" table')

        columns = [
            'id', 'name', 'grouping', 'unit'
        ]
        delete_query = 'DELETE FROM metadata.metrics'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.metrics ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "metrics" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "metrics" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_metrics_data()
    insert_metric_data(data)

ingest_mlb_metrics()