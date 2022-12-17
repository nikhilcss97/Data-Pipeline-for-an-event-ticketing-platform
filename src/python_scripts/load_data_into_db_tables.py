import psycopg2

from src.python_scripts.copy_queries import main

DATES = ["2022-10-02"]

conn = psycopg2.connect(
    database="event_ticketing_pg_db",
    user='postgres', password='123',
    host='localhost', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()

copy_queries = main(dates=DATES)

for query in copy_queries:
    cursor.execute(query)

conn.commit()
conn.close()
