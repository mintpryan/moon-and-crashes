from dotenv import load_dotenv
import os

# Загрузка переменных из .env файла
load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL", "https://data.cityofnewyork.us/resource/h9gi-nx95.json")