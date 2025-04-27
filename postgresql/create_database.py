import pandas as pd
from .db import get_connection

def create_table(table_name, csv_path):
    """
    Creates the main table and inserts data from a CSV file.
    Assumes the CSV columns match expected student table schema.
    """
    conn = get_connection()
    cur = conn.cursor()

    # Create the table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            student_id TEXT,
            course_id TEXT,
            roll_no TEXT,
            email_id TEXT,
            grade TEXT,
            PRIMARY KEY (student_id, course_id)
        )
    """)
    conn.commit()

    # Read CSV and insert rows
    df = pd.read_csv(csv_path)

    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} (student_id, course_id, roll_no, email_id, grade)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row['student-ID'],
            row['course-id'],
            row['roll no'],
            row['email ID'],
            row['grade']
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Data has been inserted successfully into {table_name}!")
