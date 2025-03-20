import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)


@dag(
    'ingest_mlb_event_types',
    description='Get the latest position data from the mlb "event_types" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata', 'deletable']
)

def ingest_mlb_event_types():
    @task(task_id='get_event_types_data')
    def get_event_types_data():
        """
        Get the latest data from the mlb "event_types" endpoint.
        """
        log.info('Calling mlb "event_types" endpoint"')

        events = mlb.get('meta', {'type': "eventTypes"})

        data = [(row['code'], row['plateAppearance'], row['hit'], row['baseRunningEvent'], row['description']) for row in events]
        return data
    
    @task(task_id='insert_event_types_data')
    def insert_event_types_data(data: list):
        """
        Insert the data into the "event_types" table.
        """
        log.info('Inserting data into the "event_types" table')

        columns = [
            'code', 'plate_appearance', 'hit', 'base_running_event', 'description'
        ]
        delete_query = 'DELETE FROM metadata.event_types'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.event_types ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "event_types" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "event_types" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_event_types_data()
    insert_event_types_data(data)

ingest_mlb_event_types()