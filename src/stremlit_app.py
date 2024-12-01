import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path


# Funkcija prisijungti prie SQLite duomenų bazės
def connect_db():
    try:
        db_path = Path("src/database.db").resolve()  # Nustatome absoliutų kelią iki duomenų bazės
        return sqlite3.connect(db_path)  # Sukuriame ryšį su duomenų baze
    except sqlite3.Error as e:
        st.error(f"Klaida jungiantis prie duomenų bazės: {e}")
        return None


# Funkcija vykdyti SQL užklausą
def execute_query(query):
    try:
        with connect_db() as conn:  # Naudojame kontekstinį valdytoją, kad automatiškai uždarytume ryšį
            if conn:
                result = pd.read_sql_query(query, conn)

                # Taisome datų formatavimą, kad būtų rodomos tik datos
                for column in result.columns:
                    if result[column].dtype in ['object', 'datetime64[ns]']:
                        try:
                            result[column] = pd.to_datetime(result[column]).dt.strftime(
                                '%Y-%m-%d')  # Formatuojame tik datas
                        except Exception:
                            pass  # Jei stulpelis nėra data, praleidžiame

                # Taisome didelių skaičių formatavimą, pašalinant skyriklius
                for column in result.select_dtypes(include=['int64', 'float64']).columns:
                    result[column] = result[column].apply(lambda x: f"{x:.0f}")  # Šaliname tūkstantinius skyriklius

                return result
    except Exception as e:
        st.error(f"Klaida vykdant užklausą: {e}")
        return None


# Funkcija nuskaityti SQL failus iš katalogo ir juos surūšiuoti abėcėlės tvarka
def load_queries_from_folder(folder_path):
    queries = {}
    try:
        sql_folder = Path(folder_path).resolve()
        sql_files = sorted(sql_folder.glob("*.sql"))  # Surūšiuojame failus abėcėlės tvarka
        for sql_file in sql_files:
            with open(sql_file, "r", encoding="utf-8") as file:
                query_name = sql_file.stem  # Failo pavadinimas be plėtinio
                queries[query_name] = file.read()
    except Exception as e:
        st.error(f"Klaida skaitant SQL užklausas iš katalogo: {e}")
    return queries


# SQL užklausos iš `sql` katalogo
queries = load_queries_from_folder("src/sql")

# Streamlit programos išdėstymas
st.title("Techninės užduoties sprendiniai")

# Šoninė juosta su užklausų mygtukais
st.sidebar.title("Pasirinkite užklausą")
for query_name, query in queries.items():
    if st.sidebar.button(query_name, key=query_name, use_container_width=True):
        st.write(f"Vykdoma užklausa: {query_name}")
        result = execute_query(query)
        if result is not None and not result.empty:
            st.write("Užklausa sėkmingai įvykdyta:")
            st.dataframe(result)
        else:
            st.error("Nėra rezultatų arba įvyko klaida vykdant užklausą.")
