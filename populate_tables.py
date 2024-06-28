from faker import Faker
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os


def check_connection(**kwargs) -> bool:
    try:
        connection = mysql.connector.connect(**kwargs)
        if connection.is_connected():
            print("Connection established")
            connection.close()
        else:
            print("Connection failed")
    except Error as e:
        print(f"Error: {e}")
        return False


def if_table_is_empty(table_name: str, **kwargs) -> bool:
    with mysql.connector.connect(**kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"select exists(select 1 from {table_name} limit 1)")
            result = cursor.fetchone()
            if result[0] == 0:
                return True
            else:
                return False


def populate_customers(
    n_customers: int, fake: Faker, batch_size: int = 5000, **kwargs
) -> None:

    if if_table_is_empty("customers", **kwargs):

        insert_query = """
        INSERT INTO customers (name, date_birth, email_address, phone_number, country)
        VALUES (%s, %s, %s, %s, %s)"""

        try:
            with mysql.connector.connect(**kwargs) as connection:
                with connection.cursor() as cursor:
                    data = []
                    for _ in range(n_customers):
                        name = fake.name()
                        data_birth = fake.date_of_birth(minimum_age=18, maximum_age=65)
                        email = fake.email()
                        phone = fake.phone_number()[:20]
                        country = fake.country()
                        data.append((name, data_birth, email, phone, country))

                        if len(data) == batch_size:
                            cursor.executemany(insert_query, data)
                            connection.commit()
                            data = []
                    if data:
                        cursor.executemany(insert_query, data)
                        connection.commit()

        except Error as e:
            print(f"Error: {e}")

    else:
        print("Table customers is already populated")


def populate_products(
    n_products: int, fake: Faker, batch_size: int = 5000, **kwargs
) -> None:

    if if_table_is_empty("products", **kwargs):

        insert_query = """
        INSERT INTO products (product_name, unit_price)
        VALUES (%s, %s)"""

        try:
            with mysql.connector.connect(**kwargs) as connection:
                with connection.cursor() as cursor:
                    data = []
                    for _ in range(n_products):
                        name = fake.word()
                        price = fake.random_int(min=1, max=1000)
                        data.append((name, price))

                        if len(data) == batch_size:
                            cursor.executemany(insert_query, data)
                            connection.commit()
                            data = []
                    if data:
                        cursor.executemany(insert_query, data)
                        connection.commit()

        except Error as e:
            print(f"Error: {e}")

    else:
        print("Table products is already populated")


def populate_channels(**kwargs) -> None:

    if if_table_is_empty("channels", **kwargs):

        insert_query = """
        INSERT INTO channels (channel_name)
        VALUES (%s)"""

        acquisition_channels = [
            "Instagram",
            "Facebook",
            "Email",
            "YouTube",
            "Organic Search",
            "Paid Search",
            "Twitter",
            "LinkedIn",
            "Referral",
            "Direct",
            "Display Ads",
            "Affiliate Marketing",
            "Snapchat",
            "TikTok",
            "Pinterest",
            "Influencer Marketing",
            "Webinars",
            "Podcasts",
        ]

        try:
            with mysql.connector.connect(**kwargs) as connection:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        insert_query, [(channel,) for channel in acquisition_channels]
                    )
                    connection.commit()

        except Error as e:
            print(f"Error: {e}")

    else:
        print("Table channels is already populated")


if __name__ == "__main__":

    load_dotenv()
    fake = Faker()

    my_sql_data = {
        "host": "localhost",
        "user": "root",
        "password": os.getenv("MY_SQL_PWD"),
        "database": "OMNI_MANAGEMENT",
    }

    populate_customers(1000, fake, **my_sql_data)
    populate_products(3000, fake, **my_sql_data)
    populate_channels(**my_sql_data)
