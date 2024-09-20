import aiohttp
import asyncio
import zipfile
import os
import io
import logging
import time  # To track execution time
logging.basicConfig(level=logging.INFO,format="%(levelname)s | %(asctime)s | %(message)s")


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def get_output_path(dir_name = "downloads"):
    cwd = os.path.dirname(os.path.abspath(__file__))
    output_path = f'{cwd}/{dir_name}'
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logging.info(f"Folder path created, Didn't exist before")
    else:
        logging.info(f"Folder path already existed")
    return output_path


async def extract_file(session,url,output_path):
    try:
        async with session.get(url,timeout=60) as response:
            response.raise_for_status()
            zip_content = io.BytesIO(await response.read())
            with zipfile.ZipFile(zip_content) as zip_file:
                zip_file.extractall(output_path)
            logging.info(f"Successfully extracted {url}")

    except aiohttp.ClientError as e:
        logging.error(f"Request failed: {e}")

    except zipfile.BadZipFile as e:
        logging.error(f"Failed to unzip file: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


async def main(download_uris,dir_name = "downloads"):

    output_path = get_output_path(dir_name)
    logging.info(f"Download folder path set at {output_path}")
    async with aiohttp.ClientSession() as session:
        tasks = []
        for uri in download_uris:
            logging.info(f"Working on {uri}")
            tasks.append(extract_file(session, uri, output_path))
        await asyncio.gather(*tasks)



if __name__ == "__main__":

    start_time = time.time()  # Start timer
    asyncio.run(main(download_uris))
    end_time = time.time()  # End timer
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")
