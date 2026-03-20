import sqlite3
from datetime import datetime
import pandas as pd

def create_db():
    conn = sqlite3.connect('properties.db')
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS rental_properties (
        property_id TEXT PRIMARY KEY,
        url TEXT,
        title TEXT,
        price_euros INTEGER,
        bedrooms INTEGER,
        bathrooms INTEGER,
        square_meters INTEGER,
        has_garage BOOLEAN,
        pets_allowed BOOLEAN,
        neighborhood_zone TEXT,
        discovery_date DATE,
        last_update DATE
    )
    """

    cursor.execute(query)
    conn.commit()

    conn.close()
    print("Database 'properties.db' and table 'rental_properties' successfully created or verified.")

def save_properties_to_db(df):
    """
    Saves a Pandas DataFrame to the SQLite database using UPSERT logic.
    - If property_id doesn't exist -> INSERT it.
    - If property_id exists -> UPDATE its price and last_update date.
    """

    if df is None or df.empty:
        print("No data to save to the database.")
        return
    
    conn = sqlite3.connect('properties.db')
    cursor = conn.cursor()

    today = datetime.now().strftime('%Y-%m-%d')

    upsert_query = """
    INSERT INTO rental_properties 
    (property_id, url, title, price_euros, bedrooms, bathrooms, square_meters, discovery_date, last_update)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(property_id) DO UPDATE SET
        price_euros = excluded.price_euros,
        title = excluded.title,
        last_update = excluded.last_update;
    """

    print("Saving data to SQLite database...")

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        p_id = row['property_id']
        url = row['link']
        title = row['title']
        price = None if pd.isna(row['price_euros']) else int(row['price_euros'])
        beds = None if pd.isna(row['bedrooms']) else int(row['bedrooms'])
        baths = None if pd.isna(row['bathrooms']) else int(row['bathrooms'])
        sqm = None if pd.isna(row['square_meters']) else int(row['square_meters'])

        cursor.execute(upsert_query, (p_id, url, title, price, beds, baths, sqm, today, today))

    conn.commit()
    conn.close()

    print(f"Successfully saved/updated {len(df)} properties in 'properties.db'.")


if __name__ == "__main__":
    create_db()