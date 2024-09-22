import requests
import logging
import json
import pandas as pd

from geopy import Nominatim
from bs4 import BeautifulSoup
from datetime import datetime

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    """
    Fetches the content of a Wikipedia page.

    This function sends a GET request to the specified Wikipedia page 
    URL and returns the response object if the request is successful. 

    Parameters:
    url (str): The URL of the Wikipedia page to fetch.

    Returns:
    response (requests.Response): The response object containing the page content 
    if the request is successful.
    """
    print("Getting wikipedia page...", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")

def get_wikipedia_data(html):
    """
    Extracts data from a Wikipedia page's HTML content.

    This function parses the provided HTML content using BeautifulSoup, locates the first sortable wikitable, 
    and retrieves all rows from the table.

    Parameters:
    html (str): The HTML content of the Wikipedia page.

    Returns:
    table_rows (ResultSet): A ResultSet of all table row elements from the first sortable wikitable.
    """   
    try:
        logging.info(f"Data retrieved from page...")
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select("table.wikitable.sortable")[0]
        table_rows = table.find_all("tr")

        return table_rows
    except Exception as e:
        logging.error(f'could not get data due to {e}')

def clean_text(text):
    """
    Cleans and formats a given text string.

    This function performs several cleaning operations on the input text:
    - Strips leading and trailing whitespace.
    - Removes occurrences of '&nbsp'.
    - Splits the text at ' ♦' and retains the part before it.
    - Splits the text at '[' and retains the part before it.
    - Splits the text at ' (formerly)' and retains the part before it.
    - Removes newline characters.

    Parameters:
    text (str): The text string to be cleaned.

    Returns:
    str: The cleaned and formatted text string.
    """
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')

def extract_wikipedia_data(**kwargs):

    """
    Extracts and processes data from a Wikipedia page.

    This function retrieves the HTML content of a Wikipedia page, extracts data from a sortable wikitable, 
    and processes the data into a structured format. 
    The processed data is then pushed to XCom for further use in an Airflow task.

    Parameters:
    kwargs (dict): A dictionary of keyword arguments, including:
        - url (str): The URL of the Wikipedia page to fetch.
        - ti (TaskInstance): The Airflow TaskInstance object for XCom operations.

    Returns:
    str: "OK" if the data extraction and processing are successful.
    """
    try:
        url = kwargs['url']
        html = get_wikipedia_page(url)
        rows = get_wikipedia_data(html)
        logging.info(f"Rows retrieved")

        data = []

        for i in range(1, len(rows)):
            tds = rows[i].find_all("td")
            values = {
                "rank": i,
                "stadium": clean_text(tds[0].text),
                "capacity": clean_text(tds[1].text).replace(',', '').replace('.', ''),
                "region": clean_text(tds[2].text),
                "country": clean_text(tds[3].text),
                "city": clean_text(tds[4].text),
                "images": 'https://' + tds[5].find("img").get("src").split("//")[1] if tds[5].find("img") else "NO_IMAGE",
                "home_team": clean_text(tds[6].text)
            }
            data.append(values)
        
        json_rows = json.dumps(data)
        kwargs['ti'].xcom_push(key='rows', value=json_rows)
        return "OK"
    except Exception as e:
        logging.error(f'could not get rows due to {e}')

def get_lat_long(country, city):
    """
    Retrieves the latitude and longitude for a given city and country.

    This function uses the Nominatim geocoding service to find the geographical coordinates 
    (latitude and longitude) of a specified city and country. 
    If the location is found, it returns the coordinates; otherwise, it returns None.

    Parameters:
    country (str): The name of the country.
    city (str): The name of the city.

    Returns:
    list: A list containing the latitude and longitude if the location is found.
    None: If the location is not found.
    """
    geolocator = Nominatim(user_agent='faragelo011@gmail.com')
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude
    
    return None

def transform_wikipedia_data(**kwargs):
    """
    Transforms and enriches Wikipedia data for stadiums.

    This function pulls extracted data from XCom, processes it into a DataFrame, 
    enriches it with geographical coordinates, and handles image and capacity data. 
    It also resolves duplicate locations by updating them with more accurate coordinates. 
    The transformed data is then pushed back to XCom for further use.

    Parameters:
    kwargs (dict): A dictionary of keyword arguments, including:
        - ti (TaskInstance): The Airflow TaskInstance object for XCom operations.

    Returns:
    str: "OK" if the data transformation is successful.
    """
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())
    return "OK"

def write_wikipedia_data(**kwargs):
    """
    Writes transformed Wikipedia data to a CSV file in Azure Data Lake.

    This function retrieves data from an XCom pushed by the 'transform_wikipedia_data' task,
    converts it from JSON to a pandas DataFrame, and writes it to a CSV file in Azure Data Lake.
    The filename includes the current date and time to ensure uniqueness.

    Parameters:
    **kwargs: Arbitrary keyword arguments. Expects 'ti' (TaskInstance) to pull XCom data.

    Returns:
    None
    """
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    
    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date()) 
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    
    # data.to_csv('data/' + file_name, index=False)
    data.to_csv('abfs://footballdata@footballdatast.dfs.core.windows.net/data/' + file_name,
                storage_options={
                    'account_key': ''
                }, index=False)