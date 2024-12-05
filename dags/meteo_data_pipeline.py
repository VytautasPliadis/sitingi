from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
from typing import List, Dict
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Konstantos
ALL_STATIONS_IDS_ENDPOINT = "https://dd5a6e97-ec69-4712-a9d1-66165374a166.mock.pstmn.io/gat_all_stations_ids"
ALL_STATIONS_DATA_ENDPOINT = "https://dd5a6e97-ec69-4712-a9d1-66165374a166.mock.pstmn.io/gat_all_stations_data"
SPECIFIC_STATION_DATA_ENDPOINT = "https://dd5a6e97-ec69-4712-a9d1-66165374a166.mock.pstmn.io/get_station?station_id={station_id}"

# Numatyti argumentai
default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'max_retry_delay': timedelta(minutes=155),
    'retry_exponential_backoff': True,
}

# DAG apibrėžimas
@dag(
    dag_id="meteo_data_pipeline",
    default_args=default_args,
    description="Gauti stočių duomenis, apdoroti trūkstamas stotis ir dinamiškai įkelti duomenis",
    schedule_interval= "@daily",
    start_date=days_ago(1),
    catchup=False,
)
def meteo_data_pipeline():
    @task
    def fetch_all_station_ids() -> List[int]:
        """Gauti visų stočių ID iš API."""
        response = requests.get(ALL_STATIONS_IDS_ENDPOINT)
        response.raise_for_status()
        stations = response.json()
        if not isinstance(stations, list):
            raise ValueError("Tikėtasi gauti stočių ID sąrašą, bet gautas kitas formatas")
        return [station["station_id"] for station in stations]

    @task
    def fetch_all_station_data() -> List[Dict]:
        """Gauti duomenis apie visas stotis."""
        response = requests.get(ALL_STATIONS_DATA_ENDPOINT)
        response.raise_for_status()
        all_station_data = response.json()
        if not isinstance(all_station_data, list):
            raise ValueError("Tikėtasi gauti stočių duomenų sąrašą iš ALL_STATIONS_DATA_ENDPOINT")
        return all_station_data

    @task
    def upload_station_data(stations: List[Dict]):
        """Įkelti stočių duomenis iš ALL_STATIONS_DATA_ENDPOINT į duomenų bazę."""
        insert_query = """
            INSERT INTO meteo_data (station_id, data, last_updated)
            VALUES (%(station_id)s, %(data)s, %(last_updated)s)
            ON CONFLICT (station_id) DO UPDATE 
            SET data = EXCLUDED.data, last_updated = EXCLUDED.last_updated;
        """

        # Duomenų paruošimas įkėlimui
        values = [
            {
                "station_id": station["station_id"],
                "data": station["data"],
                "last_updated": station["last_updated"],
            }
            for station in stations
        ]

        # Naudojamas PostgresHook duomenų įkėlimui
        postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, values)
            conn.commit()

    @task
    def identify_missing_stations(all_ids: List[int], fetched_stations: List[Dict]) -> List[int]:
        """Nustatyti trūkstamas stotis, lyginant gautus stočių duomenis su visų stočių ID sąrašu."""
        fetched_ids = [station["station_id"] for station in fetched_stations]
        missing_ids = list(set(all_ids) - set(fetched_ids))
        return missing_ids

    @task
    def fetch_and_upload_missing_station_data(station_id: int):
        """Gauti trūkstamos stoties duomenis ir įkelti juos į duomenų bazę."""
        # Gauti stoties duomenis
        url = SPECIFIC_STATION_DATA_ENDPOINT.format(station_id=station_id)
        response = requests.get(url)
        response.raise_for_status()
        station_data = response.json()

        # Įkelti stoties duomenis
        insert_query = """
            INSERT INTO meteo_data (station_id, data, last_updated)
            VALUES (%(station_id)s, %(data)s, %(last_updated)s)
            ON CONFLICT (station_id) DO UPDATE 
            SET data = EXCLUDED.data, last_updated = EXCLUDED.last_updated;
        """
        postgres_task = PostgresOperator(
            task_id=f'upload_missing_station_{station_id}',
            postgres_conn_id='postgres_connection',
            sql=insert_query,
            parameters=station_data,
        )
        postgres_task.execute(context={})

    # Užduočių priklausomybės
    all_ids = fetch_all_station_ids()
    all_station_data = fetch_all_station_data()
    upload_station_data(all_station_data)
    missing_ids = identify_missing_stations(all_ids, all_station_data)
    fetch_and_upload_missing_station_data.expand(station_id=missing_ids)


# DAG instancija
dag_instance = meteo_data_pipeline()
