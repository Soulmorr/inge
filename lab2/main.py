import os
import requests
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_and_extract(uri):
    # Отримання ім'я файлу з URI
    filename = uri.split("/")[-1]

    # Створення каталогу, якщо він не існує
    os.makedirs("lab2/downloads", exist_ok=True)

    # Завантаження файлу
    response = requests.get(uri)
    with open(f"lab2/downloads/{filename}", "wb") as file:
        file.write(response.content)

    # Розпакування zip-файлу
    with zipfile.ZipFile(f"lab2/downloads/{filename}", "r") as zip_ref:
        zip_ref.extractall("Lab2/downloads")

    # Видалення zip-файлу
    os.remove(f"lab2/downloads/{filename}")

def main():
    for uri in download_uris:
        download_and_extract(uri)

if __name__ == "__main__":
    main()
