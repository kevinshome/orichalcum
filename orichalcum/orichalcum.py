import dotenv
dotenv.load_dotenv()

import os
import json
from typing import Dict, Optional, List, Tuple
import psycopg
import click
import requests
from . import parser

API_KEY = os.environ["API_KEY"]
global glob_data_folder
playlist_ids = {
    "rock": "PLP4CSgl7K7ori6-Iz-AWcX561iWCfapt_",
    "hipHop": "PLP4CSgl7K7ormBIO138tYonB949PHnNcP",
    "pop": "PLP4CSgl7K7oqibt_5oDPppWxQ0iaxyyeq",
    "electronic": "PLP4CSgl7K7ormX2pL9h0inES2Ub630NoL",
    "loudRock": "PLP4CSgl7K7orAG2zKtoJKnTt_bAnLwTXo",
    "classics": "PLP4CSgl7K7or_7JI7RsEsptyS4wfLFGIN",
    "other": "PLP4CSgl7K7orSnEBkcBRqI5fDgKSs5c8o"
}

def fetch_50(playlist: str, token: Optional[str]=None) -> Tuple[Dict, str]:
    # fetch first page
    params_payload = {
        "key": API_KEY,
        "playlistId": playlist_ids[playlist],
        "part": "snippet",
        "maxResults": "50"
    }
    if token:
        params_payload["pageToken"] = token
    api_data = requests.get(
        url="https://www.googleapis.com/youtube/v3/playlistItems",
        params=params_payload,
    ).json()
    return api_data, api_data.get("nextPageToken")

def fetch_playlist_data(playlist_list: List[str]) -> dict:
    global glob_data_folder
    page_token: Optional[str] = None
    for playlist in playlist_list:
        list_data = []
        print(f"Fetching data from '{playlist}' playlist...")

        api_data, page_token = fetch_50(playlist)
        for item in api_data["items"]:
            list_data.append(parser.create_video_object(item))
        while page_token:
            api_data, page_token = fetch_50(playlist, page_token)
            for item in api_data["items"]:
                if "TRACK REVIEW" in item["snippet"]["title"]: # skip track reviews, we're only here for the albums
                    continue
                elif item["snippet"]["title"] == "Private video": # skip private videos, obviously
                    continue
                list_data.append(parser.create_video_object(item))
        with open(f"{glob_data_folder}/{playlist}.json", 'w') as f:
            list_data.reverse()
            f.write(json.dumps(list_data))

def create_database():
    with psycopg.connect(os.environ["DATABASE_URI"]) as conn:
        cursor = conn.cursor()
        for file in os.listdir(glob_data_folder):
            with open(f"{glob_data_folder}/{file}") as f:
                tablename = file.replace(".json", "")
                print(f"Creating table {tablename}...")
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {tablename}(
                    id character varying PRIMARY KEY,
                    artist character varying,
                    album character varying,
                    rating_data JSON
                )
                """)
                data = json.load(f)
                print(f"Populating table {tablename}...")
                for item in data:
                    cursor.execute(f"""
                    INSERT INTO {tablename} (id, artist, album, rating_data) VALUES(
                        '{item["video_id"]}',
                        '{item["artist"]}',
                        '{item["album"]}',
                        '{json.dumps(item["rating_data"])}'
                    )
                    """)
                conn.commit()
                print(f"Table {tablename} created successfully!")
        cursor.close()
    raise SystemExit(0)

@click.command()
@click.option("--playlists", default="all", help="Which (comma-seperated) playlists to fetch data from (options: rock, hipHop, pop, electronic, loudRock, classics, other)")
@click.option("--data-folder", default="data", help="Folder in which to store JSON data (default: './data')")
@click.option("--create-db", is_flag=True, help="Create a database with fetched JSON data")
def main(playlists, data_folder, create_db):
    global glob_data_folder
    glob_data_folder = data_folder
    if create_db:
        create_database()

    if not os.path.exists("data"):
        os.mkdir("data")

    if playlists == "all":
        fetch_playlist_data(playlist_ids.keys())
    else: # TODO: add sanitizer
        fetch_playlist_data(playlists.split(','))

if __name__ == "__main__":
    main()