import psycopg2
from dotenv import load_dotenv
import os

def get_connection():
    load_dotenv()
    dbname=os.environ.get("DBNAME")
    user=os.environ.get("USER")
    password=os.environ.get("PASSWORD")
    port=os.environ.get("PORT")
    if not dbname or not user or not password or not port:
        raise EnvironmentError("DBNAME or USER or PASSWORD or PORT environment variable/s is/are not set")
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host="localhost",
        port=port
    )
