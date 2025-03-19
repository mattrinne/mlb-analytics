import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)

config = {
    'mlb': {
        'sport_id': 1,
        'seasons': [ 2025 ]
    }
}

@dag(
    'ingest_mlb_positions',
    description='Get the latest position data from the mlb "positions" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_positions():
    @task(task_id='get_positions_data')
    def get_positions_data():
        """
        Get the latest data from the mlb "positions" endpoint.
        """
        log.info('Calling mlb "positions" endpoint"')

        positions = mlb.get('meta', {'type': "positions"})

        data = []
        for position in positions:
            if position['shortName'] != "Batter":
                data.append((position['code'], position['shortName'], position['fullName'], position['abbrev'], position['type'], position['formalName'], position['displayName'], position['gamePosition'], position['pitcher'], position['fielder'], position['outfield']))

        return data
    
    @task(task_id='insert_positions_data')
    def insert_positions_data(data: list):
        """
        Insert the data into the "positions" table.
        """
        log.info('Inserting data into the "positions" table')

        columns = [
            'code', 'short_name', 'full_name', 'abbreviation', 'type', 'formal_name',
            'display_name', 'game_position', 'pitcher', 'fielder', 'outfield'
        ]
        delete_query = 'DELETE FROM metadata.positions'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.positions ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "positions" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "positions" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_positions_data()
    insert_positions_data(data)

ingest_mlb_positions()