import sqlite3
import csv
import os
from datetime import datetime

def create_database(db_file_path='transactions.db'):
    """
    Create an empty SQLite database with the standard transaction schema.
    """
    # Remove existing database if it exists
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
        print(f"Removed existing database: {db_file_path}")
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    
    try:
        # Create transactions table with standard schema
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Date TEXT,
                Type TEXT,
                Sub_Type TEXT,
                Symbol TEXT,
                Instrument_Type TEXT,
                Action TEXT,
                Quantity REAL,
                Value REAL,
                Average_Price REAL,
                Total REAL,
                Commissions REAL,
                Fees REAL,
                Currency TEXT,
                Root_Symbol TEXT,
                Underlying_Symbol TEXT,
                Expiration_Date TEXT,
                Strike_Price REAL,
                Call_or_Put TEXT,
                Description TEXT,
                Asset_Category TEXT
            )
        ''')
        
        # Create exchange rates table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                usd_to_eur_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        print(f"Database created successfully: {db_file_path}")
        
    except Exception as e:
        print(f"Error creating database: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def create_database_from_csv(csv_file_path, db_file_path):
    """
    Create a SQLite database using the first line of CSV as schema
    and import all data from the CSV file.
    """
    
    # Remove existing database if it exists
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
        print(f"Removed existing database: {db_file_path}")
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    
    try:
        # Read CSV file to get headers and sample data
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)  # Get the first line as headers
            
            print(f"Found {len(headers)} columns in CSV:")
            for i, header in enumerate(headers, 1):
                print(f"  {i}. {header}")
            
            # Create table with appropriate data types
            # Based on the CSV structure, we'll define appropriate types
            column_definitions = []
            for header in headers:
                # Clean column name (replace spaces and special characters)
                clean_header = header.replace(' ', '_').replace('#', 'Number').replace('/', '_')
                
                # Determine data type based on column name
                if header.lower() in ['date', 'expiration date']:
                    column_definitions.append(f'"{clean_header}" TEXT')
                elif header.lower() in ['value', 'quantity', 'average price', 'commissions', 'fees', 'multiplier', 'strike price', 'total']:
                    column_definitions.append(f'"{clean_header}" REAL')
                elif header.lower() in ['order #']:
                    column_definitions.append(f'"{clean_header}" INTEGER')
                else:
                    column_definitions.append(f'"{clean_header}" TEXT')
            
            # Create table
            table_name = 'transactions'
            create_table_sql = f'CREATE TABLE {table_name} ({', '.join(column_definitions)})'
            
            print(f"\nCreating table with SQL:")
            print(create_table_sql)
            
            cursor.execute(create_table_sql)
            
            # Prepare insert statement
            placeholders = ', '.join(['?' for _ in headers])
            insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'
            
            # Reset file pointer to beginning and skip header
            file.seek(0)
            next(csv_reader)  # Skip header row
            
            # Insert data
            row_count = 0
            for row in csv_reader:
                # Handle empty values and convert data types
                processed_row = []
                for i, value in enumerate(row):
                    if value == '' or value == '--':
                        processed_row.append(None)
                    else:
                        # Remove commas from numeric values (like "1,076.40")
                        if headers[i].lower() in ['value', 'quantity', 'average price', 'commissions', 'fees', 'multiplier', 'strike price', 'total']:
                            try:
                                # Remove commas and convert to float
                                clean_value = value.replace(',', '')
                                processed_row.append(float(clean_value))
                            except ValueError:
                                processed_row.append(None)
                        else:
                            processed_row.append(value)
                
                cursor.execute(insert_sql, processed_row)
                row_count += 1
            
            print(f"\nInserted {row_count} rows into the database.")
            
        # Commit changes
        conn.commit()
        
        # Display some statistics
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        total_rows = cursor.fetchone()[0]
        
        cursor.execute(f'PRAGMA table_info({table_name})')
        columns_info = cursor.fetchall()
        
        print(f"\nDatabase created successfully!")
        print(f"Database file: {db_file_path}")
        print(f"Table name: {table_name}")
        print(f"Total rows: {total_rows}")
        print(f"Columns ({len(columns_info)}):")
        for col in columns_info:
            print(f"  - {col[1]} ({col[2]})")
        
        # Show sample data
        print(f"\nSample data (first 3 rows):")
        cursor.execute(f'SELECT * FROM {table_name} LIMIT 3')
        sample_rows = cursor.fetchall()
        
        for i, row in enumerate(sample_rows, 1):
            print(f"\nRow {i}:")
            for j, value in enumerate(row):
                print(f"  {headers[j]}: {value}")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    
    finally:
        conn.close()

if __name__ == "__main__":
    # File paths
    csv_file = "tastytrade_transactions_history_x5WY99502_250101_to_250721.csv"
    db_file = "transactions.db"
    
    print("Creating SQLite database from CSV file...")
    print(f"CSV file: {csv_file}")
    print(f"Database file: {db_file}")
    print("-" * 50)
    
    create_database_from_csv(csv_file, db_file)
    
    print("\nDatabase creation completed!")