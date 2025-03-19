import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)

ingest_sport_ids = [
    1, # MLB
]

@dag(
    'ingest_mlb_teams',
    description='Get the latest data from the mlb "teams" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_teams():
    @task(task_id='get_teams_data')
    def get_teams_data():
        """
        Get the latest data from the mlb "sports" endpoint.
        """
        data = []
        for sport_id in ingest_sport_ids:
            log.info('Calling mlb "teams" endpoint"')

            teams = mlb.get('teams', {'sportId': sport_id})['teams']
            
            data = data + [(row['id'], row['sport']['id'], row['division']['id'], row['name'], row['teamCode'], row['fileCode'], row['abbreviation'], row['teamName'], row['locationName'], row['active']) for row in teams]

        return data
    
    @task(task_id='insert_sports_data')
    def insert_teams_data(data: list):
        """
        Insert the data into the "sports" table.
        """
        log.info('Inserting data into the "sports" table')

        columns = ['id', 'sport_id', 'division_id', 'name', 'team_code', 'file_code', 'abbreviation', 'team_name', 'location', 'active']
        delete_query = 'DELETE FROM metadata.teams'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.teams ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "teams" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "teams" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_teams_data()
    insert_teams_data(data)

ingest_mlb_teams()