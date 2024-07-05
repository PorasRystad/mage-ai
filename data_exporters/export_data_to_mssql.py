from mage_ai.io.file import FileIO
from typing import List
import pyodbc
import sys

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_mssql(records_to_insert: List, **kwargs) -> None:
    docker_host = "mssql_server"
    docker_port = "1433"
    user = "sa"
    password = "YourStrong!Passw0rd"
    database = "kratos"
    table = "kratos.dbo.import_data"
    batch_table = "kratos.dbo.batch_table"

    connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={docker_host},{docker_port};UID={user};PWD={password};Encrypt=no'
    # Connect to MySQL database
    connection = pyodbc.connect(connectionString)
    with connection.cursor() as cursor:
        try:
            cursor.fast_executemany = True

            cursor.executemany(
                f"""
                    INSERT INTO {batch_table} (year, month, origin, crude_and_ngls, feedstocks,
                    total_primary_oils, liquified_petroleum_gas, petrol, aviation_turbine_fuel, other_kerosene, white_diesel, red_diesel, other_products,
                    total_products, total, batch_time ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                records_to_insert
                )
            connection.commit()

            merge_query = f"""
                WITH LatestSource AS (
                    SELECT
                        [aviation_turbine_fuel], [crude_and_ngls], [feedstocks], [liquified_petroleum_gas],
                        [other_kerosene], [other_products], [petrol], [red_diesel], 
                        [total], [total_primary_oils], [total_products], [white_diesel], [batch_time], [month], [year], [origin],
                        ROW_NUMBER() OVER (PARTITION BY [month], [year], [origin] ORDER BY [batch_time] DESC) AS rn
                    FROM {batch_table}
                )
                MERGE INTO {table} AS Target
                USING (SELECT * FROM LatestSource WHERE rn = 1) AS Source
                ON Target.[month] = Source.[month] AND Target.[year] = Source.[year] AND Target.[origin] = Source.[origin]
                WHEN MATCHED THEN 
                    UPDATE SET 
                        Target.[aviation_turbine_fuel] = Source.[aviation_turbine_fuel], 
                        Target.[crude_and_ngls] = Source.[crude_and_ngls],
                        Target.[feedstocks] = Source.[feedstocks], 
                        Target.[liquified_petroleum_gas] = Source.[liquified_petroleum_gas],
                        Target.[other_kerosene] = Source.[other_kerosene], 
                        Target.[other_products] = Source.[other_products],
                        Target.[petrol] = Source.[petrol], 
                        Target.[red_diesel] = Source.[red_diesel], 
                        Target.[total] = Source.[total],
                        Target.[total_primary_oils] = Source.[total_primary_oils],
                        Target.[total_products] = Source.[total_products], 
                        Target.[white_diesel] = Source.[white_diesel],
                        Target.[batch_time] = Source.[batch_time]
                WHEN NOT MATCHED THEN
                    INSERT (
                        [aviation_turbine_fuel], [crude_and_ngls], [feedstocks], [liquified_petroleum_gas],
                        [other_kerosene], [other_products], [petrol], [red_diesel], 
                        [total], [total_primary_oils], [total_products], [white_diesel], [batch_time], [month], [year], [origin]
                    ) VALUES (
                        Source.[aviation_turbine_fuel], Source.[crude_and_ngls], Source.[feedstocks], Source.[liquified_petroleum_gas],
                        Source.[other_kerosene], Source.[other_products], Source.[petrol], Source.[red_diesel], 
                        Source.[total], Source.[total_primary_oils], Source.[total_products], Source.[white_diesel], Source.[batch_time], 
                        Source.[month], Source.[year], Source.[origin]
                    );
            """
            cursor.execute(merge_query)
            connection.commit()

        except Exception as e:
            print(f"Error during table creation and data insertion: {e}")
            connection.rollback()

