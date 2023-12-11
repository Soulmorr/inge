import os
import aiohttp
import asyncio
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

async def download_and_extract(session, uri):
    try:
        async with session.get(uri) as response:
            # Отримання ім'я файлу з URI
            filename = os.path.basename(uri)

            # Створення каталогу, якщо він не існує
            os.makedirs("lab2/downloads", exist_ok=True)

            # Збереження zip-файлу
            with open(f"lab2/downloads/{filename}", "wb") as file:
                file.write(await response.read())

            # Розпакування zip-файлу
            with zipfile.ZipFile(f"lab2/downloads/{filename}", "r") as zip_ref:
                zip_ref.extractall("lab2/downloads")

            # Видалення zip-файлу
            os.remove(f"lab2/downloads/{filename}")
    except aiohttp.ClientError as e:
        print(f"Error downloading {uri}: {e}")
#перевірка на дійсніть посилання та завантаження його якшо він існує
async def validate_and_download(session, uri):
    try:
        async with session.head(uri) as response:
            if response.status == 200:
                await download_and_extract(session, uri)
            else:
                print(f"Invalid URL: {uri}")
    except aiohttp.ClientError as e:
        print(f"Error validating {uri}: {e}")

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [validate_and_download(session, uri) for uri in download_uris]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
