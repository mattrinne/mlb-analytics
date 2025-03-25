import logging
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

import statsapi as mlb


log = logging.getLogger(__name__)


@dag(
    'ingest_mlb_pitch_codes',
    description='Get the latest position data from the mlb "pitchCodes" endpoint',
    start_date=pendulum.datetime(2025, 3, 18),
    schedule=None,
    catchup=False,
    tags=['ingest', 'metadata']
)

def ingest_mlb_pitch_codes():
    @task(task_id='get_pitch_codes_data')
    def get_pitch_codes_data():
        """
        Get the latest data from the mlb "pitchCodes" endpoint.
        """
        log.info('Calling mlb "pitchCodes" endpoint"')

        pitch_codes = mlb.get('meta', {'type': "pitchCodes"})

        data = [(row['code'], row['description'], row['swingStatus'], row['swingMissStatus'], row['swingContactStatus'], row['sortOrder'], row['strikeStatus'], row['ballStatus'], row['pitchStatus'], row['pitchResultText']) for row in pitch_codes]

        return data
    
    @task(task_id='insert_pitch_codes_data')
    def insert_pitch_codes_data(data: list):
        """
        Insert the data into the "pitch_codes" table.
        """
        log.info('Inserting data into the "pitch_codes" table')

        columns = [
            'code', 'description', 'swing_status', 'swing_miss_status', 'swing_contact_status',
            'sort_order', 'strike_status', 'ball_status', 'pitch_status', 'pitch_result_text'
        ]
        delete_query = 'DELETE FROM metadata.pitch_codes'

        pg_hook = PostgresHook(postgres_conn_id='mlb-postgres')
        insert_query = f"""
            INSERT INTO metadata.pitch_codes ({', '.join(columns)})
            VALUES %s
        """

        from psycopg2.extras import execute_values
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                log.info('Deleting data from the "pitch_codes" table')
                cur.execute(delete_query)

                log.info('Inserting data into the "pitch_codes" table')
                execute_values(cur, insert_query, data, page_size=1000)
            conn.commit()

    data = get_pitch_codes_data()
    insert_pitch_codes_data(data)

ingest_mlb_pitch_codes()