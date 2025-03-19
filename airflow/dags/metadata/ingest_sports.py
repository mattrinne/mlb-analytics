import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)

@dag(
    'ingest_mlb_sports',
    description='Get the latest data from the mlb "sports" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_sports():
    @task(task_id='get_sports_data')
    def get_sports_data():
        """
        Get the latest data from the mlb "sports" endpoint.
        """
        log.info('Calling mlb "sports" endpoint"')

        sports = mlb.get('sports', {})['sports']

        data = [(row['id'], row['code'], row['name'], row['abbreviation'], row['sortOrder'], row['activeStatus']) for row in sports]
        return data
    
    @task(task_id='insert_sports_data')
    def insert_sports_data(data: list):
        """
        Insert the data into the "sports" table.
        """
        log.info('Inserting data into the "sports" table')

        columns = ['id', 'code', 'name', 'abbreviation', 'sort_order', 'active']
        delete_query = 'DELETE FROM metadata.sports'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.sports ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "sports" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "sports" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_sports_data()
    insert_sports_data(data)

ingest_mlb_sports()