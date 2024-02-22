import pandas as pd
import csv
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# read csv file to a list of dictionaries


class ConnectDB:

    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self, db_settings: dict):
        try:
            self.connection = psycopg2.connect(
                dbname=db_settings.get("db_name"),
                user=db_settings.get("db_user"),
                password=db_settings.get("db_password"),
                host=db_settings.get("db_host"),
                port=db_settings.get("db_port"),
            )
            self.cursor = self.connection.cursor()
            print("DB Connection Succesfull")
        except Exception as error:
            print(f"DB Connection Failed : {error}")

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Connection closed")


# Database settings
db_settings = {
    "db_name": os.environ.get("DATABASE_NAME"),
    "db_user": os.environ.get("DATABASE_USER"),
    "db_password": os.environ.get("DATABASE_PASSWORD"),
    "db_host": os.environ.get("DATABASE_HOST"),
    "db_port": os.environ.get("DATABASE_PORT"),
}