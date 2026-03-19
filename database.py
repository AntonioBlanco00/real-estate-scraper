import sqlite3

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


if __name__ == "__main__":
    create_db()