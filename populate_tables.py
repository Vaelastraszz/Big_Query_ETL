#!/Users/romainlejeune/Desktop/Python/Scripts/Big_Query_ETL/my_env/bin/python3

import pandas as pd
from faker import Faker
import random
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os


def check_connection(**kwargs) -> bool:
    try:
        connection = mysql.connector.connect(**kwargs)
        if connection.is_connected():
            print("Connection established")
        else:
            print("Connection failed")
    except Error as e:
        print(f"Error: {e}")
        return False


def populate_customers(n_customers: int, **kwargs) -> None:
    pass


if __name__ == "__main__":

    load_dotenv()

    my_sql_data = {
        "host": "localhost",
        "user": "root",
        "password": os.getenv("MY_SQL_PWD"),
        "database": "OMNI_MANAGEMENT",
    }

    check_connection(**my_sql_data)
