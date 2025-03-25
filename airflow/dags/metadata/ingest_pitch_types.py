import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)


@dag(
    'ingest_mlb_pitch_types',
    description='Get the latest position data from the mlb "pitchTypes" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_pitch_types():
    @task(task_id='get_pitch_types_data')
    def get_pitch_types_data():
        """
        Get the latest data from the mlb "pitchTypes" endpoint.
        """
        log.info('Calling mlb "pitchTypes" endpoint"')

        pitch_types = mlb.get('meta', {'type': "pitchTypes"})

        data = [(row['code'], row['description']) for row in pitch_types]

        return data
    
    @task(task_id='insert_pitch_types_data')
    def insert_pitch_types_data(data: list):
        """
        Insert the data into the "pitch_types" table.
        """
        log.info('Inserting data into the "pitch_types" table')

        columns = [
            'code', 'description'
        ]
        delete_query = 'DELETE FROM metadata.pitch_types'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.pitch_types ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "pitch_types" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "pitch_types" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_pitch_types_data()
    insert_pitch_types_data(data)

ingest_mlb_pitch_types()