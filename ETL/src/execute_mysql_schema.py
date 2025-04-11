import sys

import mysql.connector
from config.database_config import get_database_config

SQL_FILE_PATH = "..//sql//schema.sql"
DB_NAME = "json_db"

def connect_to_mysql(config):
    connection = mysql.connector.connect(**config)
    return connection

def create_database(cursor, database_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"Database {database_name} created if not exists")

def execute_sql_file(cursor, sql_file_path):
    with open(sql_file_path, 'r') as file:
        sql_script = file.read()

    sql_commands = [command.strip() for command in sql_script.split(";") if command.strip()]
    for commands in sql_commands:
        cursor.execute(commands)
        print(f"Command executed: {commands[:50]} ...")

def main():
    db_config = get_database_config()
    config = {k:v for k,v in db_config.items() if k != 'database' and k != 'url'}

    connection = connect_to_mysql(config)
    cursor = connection.cursor()

    create_database(cursor, DB_NAME)
    connection.database = DB_NAME

    execute_sql_file(cursor, SQL_FILE_PATH)
    connection.commit()

if __name__ == "__main__":
    main()