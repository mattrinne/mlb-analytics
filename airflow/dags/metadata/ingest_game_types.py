import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)


@dag(
    'ingest_mlb_game_types',
    description='Get the latest position data from the mlb "metrics" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_game_types():
    @task(task_id='get_game_types_data')
    def get_game_types_data():
        """
        Get the latest data from the mlb "gameTypes" endpoint.
        """
        log.info('Calling mlb "gameTypes" endpoint"')

        types = mlb.get('meta', {'type': "gameTypes"})

        data = [(row['id'], row['description']) for row in types]

        return data
    
    @task(task_id='insert_game_types_data')
    def insert_game_types_data(data: list):
        """
        Insert the data into the "game_types" table.
        """
        log.info('Inserting data into the "game_types" table')

        columns = [
            'id', 'description'
        ]
        delete_query = 'DELETE FROM metadata.game_types'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.game_types ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "game_types" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "game_types" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_game_types_data()
    insert_game_types_data(data)

ingest_mlb_game_types()