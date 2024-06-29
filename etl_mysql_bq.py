import mysql.connector as connection
import pandas as pd
import pandas_gbq as gbq
import os
from google.cloud import bigquery


def connect_bq(**kwargs) -> bigquery.Client:
    """
    Connects to BigQuery using the provided project ID.

    Args:
        **kwargs: Keyword arguments containing the BigQuery project ID.

    Returns:
        bigquery.Client: A client object for interacting with BigQuery.

    """
    project_id = kwargs.get("bq_project_id")
    print(f"Connecting to BigQuery project {project_id}")
    return bigquery.Client(project=project_id)


def extract_table_from_mysql(
    table_name: str, cnx: connection.MySQLConnection
) -> pd.DataFrame:
    """
    Extracts data from a MySQL table.

    Args:
        table_name (str): The name of the table to extract data from.
        cnx: The MySQL connection object.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the extracted data.
    """
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, cnx)


def transform_data_from_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the data in the given DataFrame by converting object columns to string type.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    object_columns = df.select_dtypes(include=["object"]).columns
    for col in object_columns:
        df[col] = df[col].astype(str)
    return df


def load_data_to_bq(
    df: pd.DataFrame,
    table_name: str,
    project_id: str,
    dataset: str,
) -> None:
    """
    Loads data from a pandas DataFrame into a BigQuery table.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be loaded.
        table_name (str): The name of the BigQuery table to load the data into.
        project_id (str): The ID of the BigQuery project.
        dataset (str): The name of the BigQuery dataset.

    Returns:
        None
    """
    full_table_name = f"{dataset}.{table_name}"
    gbq.to_gbq(
        df,
        full_table_name,
        project_id=project_id,
        if_exists="replace",
    )


def data_pipeline_mysql_to_bq(**kwargs) -> None:
    """
    Extracts data from MySQL tables, transforms it, and loads it into BigQuery.

    Args:
        **kwargs: Keyword arguments containing the following parameters:
            - host (str): MySQL host address.
            - user (str): MySQL username.
            - password (str): MySQL password.
            - database (str): MySQL database name.
            - bq_project_id (str): BigQuery project ID.
            - dataset (str): BigQuery dataset name.

    Returns:
        None
    """

    mysql_host = kwargs.get("host")
    mysql_user = kwargs.get("user")
    mysql_password = kwargs.get("password")
    mysql_database = kwargs.get("database")
    bq_project_id = kwargs.get("bq_project_id")
    bq_dataset = kwargs.get("dataset")

    try:
        with connection.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
        ) as cnx:
            all_tables = f"""select table_name from information_schema.tables where table_schema = '{mysql_database}'"""
            df_tables = pd.read_sql(all_tables, cnx)

            for table_name in df_tables["TABLE_NAME"]:
                print(f"Extracting data from MySQL table {table_name}")
                df = extract_table_from_mysql(table_name, cnx)
                print(f"Transforming data from MySQL table {table_name}")
                df = transform_data_from_table(df)
                print(f"Loading data to BigQuery table {table_name}")
                load_data_to_bq(df, table_name, bq_project_id, bq_dataset)

    except Exception as e:
        print(f"Error connecting to MySQL: {e}")


if __name__ == "__main__":

    kwargs = {
        # SQl_connection details
        "host": "localhost",
        "user": "root",
        "password": os.getenv("MY_SQL_PWD"),
        "database": "OMNI_MANAGEMENT",
        "bq_project_id": "dbt-omnichannel",
        "dataset": "omnichannel_raw",
    }

    data_pipeline_mysql_to_bq(**kwargs)
