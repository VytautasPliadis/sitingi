from typing import Type

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from sqlmodel import Session, create_engine, select, SQLModel
from src.db.sql_models import Objektai, Busenos, Planai

DATABASE_URL = "sqlite:///local.db"
engine = create_engine(DATABASE_URL)

# Funkcija duomenims iš API gauti
def fetch_data_from_api(start_date, end_date):
    api_url = "https://example.com/api"
    response = requests.get(api_url, params={"start_date": start_date, "end_date": end_date})
    response.raise_for_status()
    return response.json()

# Funkcija duomenims įkelti į duomenų bazę (Objektai lentelę)
def upsert_data_to_table(session: Session, table: Type[SQLModel], data: list, unique_field: str):
    """
    Bendra funkcija įrašų įkėlimui arba atnaujinimui SQLModel lentelėje.

    :param session: SQLModel sesija
    :param table: Lentelės klasė (Objektai, Busenos ir t.t.)
    :param data: API duomenų sąrašas
    :param unique_field: Unikalus laukas pagal kurį tikrinami esami įrašai
    """
    for item in data:
        statement = select(table).where(getattr(table, unique_field) == item[unique_field])
        existing_entry = session.exec(statement).first()

        if existing_entry:
            for key, value in item.items():
                setattr(existing_entry, key, value)
        else:
            new_entry = table(**item)
            session.add(new_entry)

    session.commit()

# Funkcija vykdymo užduočiai
def execute_task_for_date(execution_date, table, unique_field):
    start_date = execution_date.strftime("%Y-%m-%d")
    end_date = (execution_date + timedelta(days=1)).strftime("%Y-%m-%d")
    data = fetch_data_from_api(start_date, end_date)

    with Session(engine) as session:
        upsert_data_to_table(session, table, data, unique_field)

# Airflow DAG konfigūracija
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "data_ingestion_dag",
    default_args=default_args,
    description="Duomenų įkėlimas iš API į SQLite",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    fetch_and_insert_objektai_task = PythonOperator(
        task_id="fetch_and_insert_objektai",
        python_callable=execute_task_for_date,
        op_args=["{{ ds }}", Objektai, "obj_numeris"],
    )

    fetch_and_insert_busenos_task = PythonOperator(
        task_id="fetch_and_insert_busenos",
        python_callable=execute_task_for_date,
        op_args=["{{ ds }}", Busenos, "busena_id"],
    )

    fetch_and_insert_planai_task = PythonOperator(
        task_id="fetch_and_insert_planai",
        python_callable=execute_task_for_date,
        op_args=["{{ ds }}", Planai, "pln_id"],
    )

    fetch_and_insert_objektai_task >> fetch_and_insert_busenos_task >> fetch_and_insert_planai_task