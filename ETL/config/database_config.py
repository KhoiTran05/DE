import os
from dotenv import load_dotenv
from urllib.parse import urlparse


def get_database_config():
    load_dotenv()

    jdbc_url = os.getenv("DB_URL")
    if not jdbc_url:
        raise ValueError("Missing DB_URL environment variable in .env file")

    parsed_url = urlparse(jdbc_url.replace("jdbc:", "", 1))

    host = parsed_url.hostname
    port = parsed_url.port
    database = parsed_url.path[1:] if parsed_url.path else None

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "url": jdbc_url
    }
