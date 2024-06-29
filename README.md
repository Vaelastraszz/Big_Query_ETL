# Big_Query_ETL

# BigQuery ETL Project

This project provides tools for performing ETL (Extract, Transform, Load) operations between a MySQL database and Google BigQuery. It includes scripts to populate the MySQL database with synthetic data using the Faker library and transfer this data to BigQuery for analysis.

## Features

- **Check MySQL Connection**: Verify connectivity to the MySQL database.
- **Populate MySQL Tables**: Generate and insert synthetic data into MySQL tables.
- **ETL Pipeline**: Extract data from MySQL, transform it, and load it into BigQuery.

## Installation

1. **Clone the repository**:

    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2. **Set up the environment**:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

3. **Set up environment variables**:

    Create a `.env` file in the project root with the following content:

    ```ini
    MY_SQL_PWD=<your_mysql_password>
    ```

## Usage

### Populating MySQL Database

The following functions are used to populate the MySQL database with synthetic data:

- `populate_customers(n_customers: int, fake: Faker, batch_size: int = 5000, **kwargs) -> None`
- `populate_products(n_products: int, fake: Faker, batch_size: int = 5000, **kwargs) -> None`
- `populate_channels(**kwargs) -> None`
- `populate_purchaseHistory(n_orders: int, batch_size: int = 10000, **kwargs) -> None`
- `populate_visitHistory(n_visits: int, batch_size: int = 5000, **kwargs) -> None`

To run these functions, use the script provided in your Python environment.

### ETL Pipeline: MySQL to BigQuery

This ETL pipeline extracts data from MySQL tables, transforms it, and loads it into BigQuery.

- **Connect to BigQuery**:

    ```python
    from google.cloud import bigquery

    def connect_bq(**kwargs) -> bigquery.Client:
        # Function definition here
    ```

- **Extract Data from MySQL**:

    ```python
    import pandas as pd
    import mysql.connector as connection

    def extract_table_from_mysql(table_name: str, cnx: connection.MySQLConnection) -> pd.DataFrame:
        # Function definition here
    ```

- **Transform Data**:

    ```python
    def transform_data_from_table(df: pd.DataFrame) -> pd.DataFrame:
        # Function definition here
    ```

- **Load Data to BigQuery**:

    ```python
    import pandas_gbq as gbq

    def load_data_to_bq(df: pd.DataFrame, table_name: str, project_id: str, dataset: str) -> None:
        # Function definition here
    ```

- **Run the Data Pipeline**:

    ```python
    def data_pipeline_mysql_to_bq(**kwargs) -> None:
        # Function definition here

    if __name__ == "__main__":
        # Execution script here
    ```

### Truncating MySQL Tables

To truncate tables while avoiding foreign key issues, execute the following SQL commands:

```sql
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE visitHistory;
SET FOREIGN_KEY_CHECKS = 1;
