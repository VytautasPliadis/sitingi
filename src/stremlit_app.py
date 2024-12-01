import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path


# Connect to SQLite database
def connect_db():
    try:
        db_path = Path("src/database.db").resolve()
        connection = sqlite3.connect(db_path)
        return connection
    except sqlite3.Error as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Function to execute a query
def execute_query(query):
    conn = connect_db()
    if conn:
        try:
            result = pd.read_sql_query(query, conn)
            conn.close()
            return result
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return None
    return None

# Streamlit app layout
st.title("SQLite Database Query App")

# Input for SQL query
query = st.text_area("Enter your SQL query:", height=200, placeholder="SELECT * FROM table_name;")

# Execute button
if st.button("Execute Query"):
    if query.strip():
        st.write("Executing query...")
        result = execute_query(query)
        if result is not None:
            st.write("Query executed successfully! Here are the results:")
            st.dataframe(result)
        else:
            st.error("No results returned or error in query execution.")
    else:
        st.warning("Please enter a SQL query.")

