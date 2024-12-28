import requests
import pandas as pd
import datetime
import ephem
import logging

BASE_URL = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'
BATCH_SIZE = 1000
def fetch_crash_data(start_date: datetime.datetime, end_date: datetime.datetime,output_file:str):
    str_start_date = start_date.isoformat()
    str_end_date = end_date.isoformat()
    logging.info(f"Start extract from {str_start_date} to {str_end_date}!")
    offset = 0
    all_data = []
    while True:
        url = f"{BASE_URL}?$where=crash_date between '{str_start_date}' and '{
            str_end_date}'&$limit={BATCH_SIZE}&$offset={offset}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            all_data.extend(data)
            offset += BATCH_SIZE
            logging.info("Add new data for!")
        else:
            raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

    df = pd.DataFrame(all_data)
    df.to_csv(output_file, index=False)


def generate_moon_phases(start_date: datetime.date, end_date: datetime.date,output_file:str):

    phases = []
    date = start_date

    while date <= end_date:
        moon_phase = ephem.Moon(date).phase
        phases.append({"date": date.strftime(
            "%Y-%m-%d"), "moon_phase": moon_phase})
        date += datetime.timedelta(days=1)

    df = pd.DataFrame(phases)
    df.to_csv(output_file, index=False)
    
    