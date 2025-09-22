# etl_pipeline.py
# Import the necessary libraries
import pandas as pd
from sqlalchemy import create_engine
import os

print("All libraries imported successfully!")
print("\nStarting EXTRACT phase...")

# Define the path to your CSV file. Make sure the file 'sales_data.csv' is in the same folder as this script.
file_path = 'sales_data.csv'


# Read the CSV file into a Pandas DataFrame
# A DataFrame is like a super-powered Excel table inside Python
try:
    raw_data = pd.read_csv(file_path, encoding='latin-1')
    print(f"Successfully loaded data from {file_path}")
    print(f"Data Shape: {raw_data.shape[0]} rows, {raw_data.shape[1]} columns")
    # Let's peek at the first 5 rows to make sure it looks right
    print("\nFirst 5 rows of raw data:")
    print(raw_data.head())

except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found. Please make sure it exists.")
    exit() # This stops the script if the file is missing

print("Extract phase complete!\n")

# --- TRANSFORM PHASE ---
print("Starting TRANSFORM phase...")
clean_data = raw_data.copy()

# 1. Handle Missing Values
initial_count = len(clean_data)
clean_data.dropna(inplace=True)
final_count = len(clean_data)
print(f"Dropped {initial_count - final_count} rows with missing values.")

# 2. Ensure Correct Data Types - USING YOUR ACTUAL COLUMN NAMES!
clean_data['Order Date'] = pd.to_datetime(clean_data['Order Date'], format='%d-%m-%Y')  # Format: DD-MM-YYYY
clean_data['Ship Date'] = pd.to_datetime(clean_data['Ship Date'], format='%d-%m-%Y')
clean_data['Sales'] = clean_data['Sales'].astype(float)
clean_data['Quantity'] = clean_data['Quantity'].astype(int)
clean_data['Discount'] = clean_data['Discount'].astype(float)
clean_data['Profit'] = clean_data['Profit'].astype(float)

print("Converted data types for date and numeric columns.")

# 3. No need to create Total_Sale column - you already have 'Sales'!
# The 'Sales' column already represents the total sale amount

# 4. Check for duplicates using 'Order ID'
duplicates = clean_data.duplicated(subset=['Order ID']).sum()
print(f"Found {duplicates} duplicate Order IDs.")
if duplicates > 0:
    clean_data.drop_duplicates(subset=['Order ID'], inplace=True)
    print("Duplicates removed.")

print("\nFirst 5 rows of CLEANED data:")
print(clean_data.head())
print("Transform phase complete!\n")

# --- LOAD PHASE ---
print("Starting LOAD phase...")

# 1. Create a connection to an SQLite database.
# This will create a file called 'sales_database.db' in your project folder.
# If the file doesn't exist, SQLite will create it automatically.
database_name = "sales_database.db"
engine = create_engine(f'sqlite:///{database_name}')

# 2. Load the clean DataFrame into a new table in the database.
# We'll name the table 'fact_sales' (a common naming convention for data warehouse tables).
table_name = "fact_sales"

try:
    clean_data.to_sql(
        name=table_name,    # Name of the SQL table to create
        con=engine,         # The connection to the database we created
        index=False,        # Don't write the DataFrame's index as a column
        if_exists='replace' # If the table exists, replace it. Use 'append' to add new data.
    )
    print(f"Success! Data loaded into table '{table_name}' in database '{database_name}'.")

except Exception as e:
    print(f"An error occurred while loading data: {e}")

print("Load phase complete!")
print("\nETL Pipeline finished successfully! 🎉")

# --- EXPORT TO EXCEL ---
print("\nExporting to Excel for easy analysis...")
try:
    excel_file = 'superstore_analysis.xlsx'
    clean_data.to_excel(excel_file, index=False, engine='openpyxl')
    print(f"Success! Data exported to '{excel_file}'")
    print("You can open this file in Microsoft Excel for analysis!")
    
except Exception as e:
    print(f"Error exporting to Excel: {e}")

print("\nETL Pipeline completed successfully! 🎉")