import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname="your_dbname",
        user="your_username",
        password="your_password",
        host="localhost",
        port="5432"
    )
