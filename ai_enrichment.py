import sqlite3
import os
import time
import requests
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=API_KEY)

def get_properties_to_enrich():
    conn = sqlite3.connect('properties.db')

    conn.row_factory = sqlite3.Row 
    cursor = conn.cursor()

    query = """
    SELECT property_id, url 
    FROM rental_properties 
    WHERE has_garage IS NULL
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]

def fetch_property_description(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        desc_div = (
            soup.find('div', class_='description__content') or 
            soup.find('div', id='description') or 
            soup.find('div', class_='ad-comments')
        )
        
        if desc_div:
            return desc_div.get_text(separator=" ", strip=True)
            
        # Fallback if specific classes are not found
        paragraphs = soup.find_all('p')
        full_text = " ".join([p.get_text(strip=True) for p in paragraphs])
        
        # Limit text length to avoid sending massive HTML junk to OpenAI (saving tokens)
        return full_text[:3000] 
        
    except Exception as e:
        print(f"Error fetching description for {url}: {e}")
        return None


def analyze_with_ai(description):
    if not description or len(description) < 20:
        return {"has_garage": None, "pets_allowed": None}

    system_prompt = (
        "You are a real estate data extraction assistant. "
        "Analyze the provided property description in Spanish. "
        "Output strictly valid JSON with exactly two keys: 'has_garage' (boolean) and 'pets_allowed' (boolean). "
        "If the description explicitly mentions a garage or parking space, set 'has_garage' to true, otherwise false. "
        "If it explicitly allows pets, set 'pets_allowed' to true. If it strictly forbids them, set to false. "
        "If there is absolutely no mention of pets, set 'pets_allowed' to null."
    )

    try:
        response = client.chat.completions.create(
            model="gpt-5-mini-2025-08-07",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Description:\n\n{description}"}
            ],
            response_format={"type": "json_object"},
        )
        
        # Parse the JSON string returned by the model into a Python dictionary
        result_json = json.loads(response.choices[0].message.content)
        return result_json

    except Exception as e:
        print(f"OpenAI API Error: {e}")
        return {"has_garage": None, "pets_allowed": None}
    

def update_property_in_db(property_id, ai_data):
    conn = sqlite3.connect('properties.db')
    cursor = conn.cursor()
    
    query = """
    UPDATE rental_properties 
    SET has_garage = ?, pets_allowed = ? 
    WHERE property_id = ?
    """
    
    # We use .get() which returns None if the key is missing or explicitly None
    garage = ai_data.get('has_garage')
    pets = ai_data.get('pets_allowed')
    
    cursor.execute(query, (garage, pets, property_id))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    if not API_KEY:
        print("ERROR: OPENAI_API_KEY not found in .env file.")
        exit()
        
    print("Starting AI Enrichment Process...")
    pending_properties = get_properties_to_enrich()

    if not pending_properties:
        print("All properties are already enriched. Nothing to do!")
        exit()
        
    total = len(pending_properties)
    print(f"🔍 Found {total} properties pending enrichment.\n")

    for index, prop in enumerate(pending_properties, start=1):
        p_id = prop['property_id']
        url = prop['url']
        
        print(f"[{index}/{total}] 🌐 Scraping deep info for ID: {p_id}...")
        description = fetch_property_description(url)
        
        if description:
            ai_data = analyze_with_ai(description)
            print(f" AI Result -> Garage: {ai_data.get('has_garage')} | Pets: {ai_data.get('pets_allowed')}")
            
            # --- NUEVO: Guardamos en la base de datos ---
            update_property_in_db(p_id, ai_data)
        else:
            print("No description found. Skipping update.")
            
        print("-" * 50)
        
        # Polite delay to avoid IP blocks from the real estate portal
        time.sleep(4)
        
    print("\n AI ENRICHMENT COMPLETED! All properties updated in the database.")