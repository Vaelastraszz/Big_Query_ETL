import mysql.connector as connection
import pandas as pd
import pandas_gbq as gbq
import os
from google.cloud import bigquery


def connect_bq(**kwargs) -> bigquery.Client:
    project_id = kwargs.get("bq_project_id")
    print(f"Connecting to BigQuery project {project_id}")
    return bigquery.Client(project=project_id)


def extract_table_from_mysql(table_name: str, cnx) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, cnx)


def transform_data_from_table(df: pd.DataFrame) -> pd.DataFrame:
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
    full_table_name = f"{dataset}.{table_name}"
    gbq.to_gbq(
        df,
        full_table_name,
        project_id=project_id,
        if_exists="replace",
    )


def data_pipeline_mysql_to_bq(**kwargs) -> None:

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
