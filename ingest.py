import pandas as pd
import pyodbc
import os
import argparse
from sqlalchemy import create_engine, text
import urllib.parse

def load_csv_to_sql_server(csv_path, table_name, server='127.0.0.1', database='pythondemo', 
                          username='sa', password="P@ssw0rd!", trusted_connection=False,
                          delimiter=',', encoding='utf-8', if_exists='replace'):
    """Load data from a CSV file into SQL Server"""
    
    # Check if file exists
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
    # Read CSV file
    print(f"Reading data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, delimiter=delimiter, encoding=encoding)
        if df.empty:
            print("WARNING: The CSV file has no data!")
            return
        print(f"CSV loaded: {len(df)} rows and {len(df.columns)} columns")
        print(f"Column names: {', '.join(df.columns.tolist())}")
        print(f"First few rows: \n{df.head(3)}")
    except Exception as e:
        print(f"Error reading CSV: {str(e)}")
        return
    
    # Create connection string
    if trusted_connection:
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    else:
        if not username or not password:
            raise ValueError("Username and password are required when not using trusted connection")
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    try:
        # Connect to SQL Server
        print("Connecting to SQL Server...")
        conn = pyodbc.connect(connection_string)
        
        # Create SQLAlchemy engine for pandas to_sql method
        quoted_params = urllib.parse.quote_plus(connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted_params}")
        
        # Load data into SQL Server
        print(f"Loading data into {table_name} table...")
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        
        # Verify data was inserted - FIXED SECTION
        try:
            # Method 1: Using text() with SQLAlchemy 2.0+ syntax
            with engine.connect() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM [{table_name}]"))
                count = result.scalar()
                print(f"Verification: Table has {count} rows")
        except Exception as ve:
            print(f"Verification error: {str(ve)}")
            print("Trying alternative verification method...")
            try:
                # Method 2: Use pandas to read from the table
                df_verify = pd.read_sql(f"SELECT COUNT(*) AS row_count FROM [{table_name}]", engine)
                print(f"Verification: Table has {df_verify.iloc[0]['row_count']} rows")
            except Exception as ve2:
                print(f"Alternative verification failed: {str(ve2)}")
                print("Data may have been inserted, but verification was not possible")
            
        print(f"Successfully loaded {len(df)} rows into {table_name}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Close connection
        if 'conn' in locals():
            conn.close()
            print("SQL Server connection closed")

def main():
    parser = argparse.ArgumentParser(description='Load CSV data into SQL Server')
    parser.add_argument('csv_file', help='Path to the CSV file')
    parser.add_argument('table_name', help='Name of the target SQL Server table')
    parser.add_argument('--server', default='127.0.0.1', help='SQL Server address (default: 127.0.0.1)')
    parser.add_argument('--database', default='pythondemo', help='Database name (default: pythondemo)')
    parser.add_argument('--username', help='SQL Server username')
    parser.add_argument('--password', help='SQL Server password')
    parser.add_argument('--trusted-connection', action='store_true', help='Use Windows Authentication')
    parser.add_argument('--delimiter', default=',', help='CSV delimiter character (default: ,)')
    parser.add_argument('--if-exists', default='replace', choices=['fail', 'replace', 'append'],
                       help='How to behave if the table exists (default: replace)')
    
    args = parser.parse_args()
    
    load_csv_to_sql_server(
        csv_path=args.csv_file,
        table_name=args.table_name,
        server=args.server,
        database=args.database,
        username=args.username or 'sa',
        password=args.password or 'P@ssw0rd!',
        trusted_connection=args.trusted_connection,
        delimiter=args.delimiter,
        if_exists=args.if_exists
    )

if __name__ == "__main__":
    main()
