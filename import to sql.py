import pandas as pd               #The `pandas` library is imported to handle the CSV file reading and DataFrame operations.
import mysql.connector            #The `mysql.connector` library is imported to connect and interact with the MySQL database.
from mysql.connector import Error #The `Error` class is imported from `mysql.connector` to handle any errors that may occur.
import os                         #The `os` library is imported to extract the base name of the file being imported.

def import_csv_to_mysql(filepaths, table_names, host, user, password, database):
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(

            host='localhost',
            user='root',
            password='1234',
            database='dstask'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Iterate over each file
            for filepath, table_name in zip(filepaths, table_names):
                print(f"Importing data from {os.path.basename(filepath)} into {table_name} table...")
                
                # Read the CSV file into a pandas DataFrame
                df = pd.read_csv(filepath)

                # Create a table to import the data
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                for column in df.columns:     #This loop iterates over each column Based on the data type of the column.
                    column_type = df[column].dtype
                    if column_type == 'int64':
                        create_table_query += f"{column} INT,"
                    elif column_type == 'float64':
                        create_table_query += f"{column} FLOAT,"
                    else:
                        create_table_query += f"{column} VARCHAR(255),"
                create_table_query = create_table_query[:-1] + ")"

                cursor.execute(create_table_query)           #execute the create_table_query SQL statement and create the table in the MySQL database.

                # Insert the data into the table row by row
                for _, row in df.iterrows():
                    values = []
                    for column in df.columns:
                        value = row[column]
                        if pd.isnull(value):
                            value = None
                        values.append(value)

                    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
                    cursor.execute(insert_query, values)

                print(f"Data imported from {os.path.basename(filepath)} into {table_name} table")

            # Commit the changes and close the cursor
            connection.commit()
            cursor.close()
            print("All data imported successfully!")

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed")

# Usage example
filepaths = [
    'Orders.csv',
    'Products.csv',
    'Order Items.csv',
    'Customers.csv',
    'Geolocation.csv',
    'Order Payments.csv',
    'Reviews.csv',
    'Sellers.csv',
    'Categories.csv'
]
table_names = [
    'orders',
    'products',
    'order_items',
    'customers',
    'geolocation',
    'order_payments',
    'reviews',
    'sellers',
    'categories'
]
host='localhost'
user='root'
password='1234'
database='dstask'


import_csv_to_mysql(filepaths, table_names, host, user, password, database)
