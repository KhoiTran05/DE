import mysql.connector

db_config = {
    "host":"localhost",
    "port":3307,
    "user":"root",
    "password":"NewPassword123!"
}

db_name = "json_db"
sql_file_path = "E:\\DE-HW\\DE\\ETL\\sql\\schema.sql"

connection = mysql.connector.connect(**db_config)
 
cursor = connection.cursor()
print("Successfully connected to the database")

cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
print(f"Create {db_name} database successfully if not exists")
connection.database = db_name

with open(sql_file_path, 'r') as file:
    sql_script = file.read()
    
sql_queries = sql_script.split(';')

for query in sql_queries:
    if query.strip():
        cursor.execute(query)
        print(f"Execute {query.strip()[:50]} ...")

connection.commit()

cursor.close()
connection.close()