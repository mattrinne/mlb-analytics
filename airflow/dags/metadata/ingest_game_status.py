import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)


@dag(
    'ingest_mlb_game_status',
    description='Get the latest position data from the mlb "gameStatus" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_game_status():
    @task(task_id='get_game_status_data')
    def get_game_status_data():
        """
        Get the latest data from the mlb "gameStatus" endpoint.
        """
        log.info('Calling mlb "gameStatus" endpoint"')

        statuses = mlb.get('meta', {'type': "gameStatus"})

        data = [(row['statusCode'], row['abstractGameState'], row['codedGameState'], row['detailedState'], row['abstractGameCode']) for row in statuses]

        return data
    
    @task(task_id='insert_game_status_data')
    def insert_game_status_data(data: list):
        """
        Insert the data into the "game_statuses" table.
        """
        log.info('Inserting data into the "game_statuses" table')

        columns = [
            'status_code', 'abstract_game_state', 'coded_game_state',
            'detailed_state', 'abstract_game_code'
        ]
        delete_query = 'DELETE FROM metadata.game_statuses'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.game_statuses ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "game_statuses" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "game_statuses" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_game_status_data()
    insert_game_status_data(data)

ingest_mlb_game_status()