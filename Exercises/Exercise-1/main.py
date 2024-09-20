import requests
import zipfile
import os
import io
import logging
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
    return f'{output_path}'


def extract_file(url,dir_name = "downloads"):
    cwd = os.path.dirname(os.path.abspath(__file__))
    output_path = f'{cwd}/{dir_name}'
    
    try:
        with requests.Session() as session:
            with session.get(url, stream=True, timeout=60) as response:
                response.raise_for_status()
                zip_content = io.BytesIO(response.content)
                with zipfile.ZipFile(zip_content) as zip_file:
                    zip_file.extractall(output_path)

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")

    except zipfile.BadZipFile as e:
        logging.error(f"Failed to unzip file: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def main(download_uris,dir_name = "downloads"):

    path = get_output_path(dir_name)
    logging.info(f"Download folder path set at {path}")
    for uri in download_uris:
        logging.info(f"Working on {uri}")
        extract_file(uri,dir_name)



if __name__ == "__main__":
    main(download_uris)
