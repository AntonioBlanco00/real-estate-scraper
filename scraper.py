import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import re

def fetch_properties(url):
    # We define "Headers" so the website thinks we are a real browser, not a bot.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
    }

    print(f"Fetching data from: {url}...")
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        return soup

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def parse_properties(soup):
    if not soup:
        return []
    
    property_cards = soup.find_all('div', class_='ad-preview')
    if len(property_cards) == 0:
        return []
   
    print(f"Found {len(property_cards)} properties on this page.\n")

    properties_list = []

    # Loop through each card and extract basic info
    for card in property_cards:
        try:
            title = card.find('a', class_='ad-preview__title').text.strip()
            price_text = card.find('span', class_='ad-preview__price').text.strip()
            link_parcial = card.get('data-lnk-href')
            link = f"https://www.pisos.com{link_parcial}" if link_parcial else "No link"

            property_id = link_parcial.split('-')[-1].replace('/', '') if link_parcial else "No ID"

            characteristics = card.find_all('p', class_='ad-preview__char')

            char_text = " | ".join([char.text.strip() for char in characteristics])
            
            prop_data = {
                "property_id": property_id,
                "title": title,
                "price_raw": price_text,
                "details_raw": char_text,
                "link": link
            }

            properties_list.append(prop_data)
            
        except AttributeError:
            continue

    return properties_list

def clean_data(raw_properties_list):
    df = pd.DataFrame(raw_properties_list)

    if df .empty:
        return None
    
    print("Cleaning data with Pandas adn Regex...")

    # Price cleaning
    df['price_euros'] = (
        df['price_raw']
        .str.replace('.', '', regex=False)
        .str.extract(r'(\d+)')
        .astype('Int64')
    )

    # Extract details
    df['bedrooms'] = df['details_raw'].str.extract(r'(\d+)\s*hab').astype('Int64')
    df['bathrooms'] = df['details_raw'].str.extract(r'(\d+)\s*baño').astype('Int64')
    df['square_meters'] = df['details_raw'].str.extract(r'(\d+)\s*m').astype('Int64')

    print("CLEAN DATA (Before vs After)")
    # We select specific columns to show the transformation
    columns_to_show = ['price_raw', 'price_euros', 'bedrooms', 'bathrooms', 'square_meters']
    print(df[columns_to_show].head(5))

    return df

if __name__ == "__main__":
    BASE_URL = "https://www.pisos.com/alquiler/pisos-caceres_capital/"
    current_page =  1

    all_properties = []

    # Initialize the pagination loop to crawl through all available pages
    while True:
        # Construct the target URL dynamically based on the current page index
        if current_page == 1:
            url_to_scrape = BASE_URL
        else:
            url_to_scrape = f"{BASE_URL}{current_page}/"
        
        print(f"--- SCRAPING PAGE {current_page} ---")

        # Execute the extraction pipeline for the current page
        html_soup = fetch_properties(url_to_scrape)
        page_properties = parse_properties(html_soup)

        if len(page_properties) == 0:
            print("No more properties found.")
            break
        
        all_properties.extend(page_properties)

        # Polite delay to prevent rate-limiting or IP bans from the server
        print("Waiting 2 seconds before the next page...")
        time.sleep(2)

        current_page += 1

    print(f"Total properties extracted: {len(all_properties)}")

    if len(all_properties) > 0:
        cleaned_df = clean_data(all_properties)
    
