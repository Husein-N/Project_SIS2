import sqlite3
import pandas as pd
import os
from pathlib import Path
def load_data():
    # Step 1: Load cleaned CSV
    csv_path = "cleaned_data.csv"
    if not os.path.exists(csv_path):
        print(f" File not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    print(f" Loaded {len(df)} rows from {csv_path}")

    # Step 2: Connect to SQLite database
    db = Path("data/output.db")

    # Ensure folder exists
    db.parent.mkdir(parents=True, exist_ok=True)
    
    #db = Path("data\output.db")
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # Step 3: Create 'stocks' table 
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stocks (
        Ticker TEXT,
        Name TEXT,
        Exchange TEXT,
        Industry TEXT,
        Market_Cap REAL,
        MarketCap_Digit REAL,
        PE_Ratio REAL,
        PEG_Ratio REAL,
        Price REAL
    );
    """
    cursor.execute(create_table_query)

    # Step 4: Insert data
    insert_query = """
    INSERT INTO stocks (Ticker, Name, Exchange, Industry,
                        Market_Cap,MarketCap_Digit, PE_Ratio, PEG_Ratio, Price)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    data_to_insert = df.values.tolist()
    cursor.executemany(insert_query, data_to_insert)

    # Step 5: Commit and close
    conn.commit()
    print(f" Inserted {cursor.rowcount} rows into 'stocks' table.")

    conn.close()
    print(" Database connection closed.")

if __name__ == "__main__":
    load_data()
