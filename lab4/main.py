import os
import csv
import psycopg2
from psycopg2 import sql

def generate_create_script(file_path, table_name):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        header = next(reader)
        columns = [f"{col} VARCHAR" for col in header]
        script = f"CREATE TABLE {table_name} ({', '.join(columns)}, PRIMARY KEY ({header[0]}));"
    return script

def execute_sql_script(conn, script):
    with conn.cursor() as cursor:
        cursor.execute(script)
    conn.commit()

def insert_data_into_table(conn, table_name, file_path):
    with conn.cursor() as cursor:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            for row in reader:
                insert_query = sql.SQL("INSERT INTO {} VALUES ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Literal, row))
                )
                cursor.execute(insert_query)
    conn.commit()

def main():
    host = 'postgres'
    database = 'lab4'
    user = 'postgres'
    password = 'postgres'
    #conn = psycopg2.connect(host=host, database=database, user=user, password=password)
    conn = psycopg2.connect(dbname='lab4', user='postgres', 
                        password='postgres', host='localhost')
    data_folder = "./lab4/data"
    files = ["accounts.csv", "products.csv", "transactions.csv"]

    for file in files:
        table_name = os.path.splitext(file)[0]
        file_path = os.path.join(data_folder, file)

        # Execute CREATE script
        execute_sql_script(conn, generate_create_script(file_path, table_name))

        # Insert data into the table
        insert_data_into_table(conn, table_name, file_path)

    conn.close()

if __name__ == "__main__":
    main()
