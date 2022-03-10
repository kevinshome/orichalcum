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
logfile = open("log.txt", 'a')

def fetch_50(token: Optional[str]=None) -> Tuple[Dict, str]:
    # fetch first page
    params_payload = {
        "key": API_KEY,
        "playlistId": "UUt7fwAhXDy3oNFTAzF2o8Pw",
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

def fetch_playlist_data() -> dict:
    global page_token
    global count
    page_token = None
    list_data = []
    count = 0
    print(f"Fetching data from 'Uploads from theneedledrop' playlist...")

    def _fetch(_page_token: Optional[str]=None):
        global page_token
        global count
        api_data, page_token = fetch_50(_page_token)
        count += 50

        os.system("clear")
        if page_token:
            print(f"Progress: {count}/{api_data['pageInfo']['totalResults']}")
        else:
            print(f"Progress: {api_data['pageInfo']['totalResults']}/{api_data['pageInfo']['totalResults']}")

        for item in api_data["items"]:
            t: str = item["snippet"]["title"]
            if "review" not in t.lower(): # since we're pulling from uploads playlist, we need to skip any non-review vids
                continue
            if "track review" in t.lower(): # skip track reviews, we're only here for the albums
                continue
            elif t == "Private video": # skip private videos, obviously
                continue
            _obj = parser.create_video_object(item, logfile)
            if _obj:
                list_data.append(_obj)

    _fetch()
    while page_token:
        _fetch(page_token)

    with open("reviews.json", 'w') as f:
        list_data.reverse()
        f.write(json.dumps(list_data))

def create_database():
    with psycopg.connect(os.environ["DATABASE_URI"]) as conn:
        cursor = conn.cursor()
        print(f"Creating table 'reviews'...")
        cursor.execute("DROP TABLE IF EXISTS reviews")
        cursor.execute("""
        CREATE TABLE reviews(
            id character varying PRIMARY KEY,
            artist character varying,
            album character varying,
            rating_data JSON
        )
        """)
        with open("reviews.json") as f:
            data = json.load(f)
            print(f"Populating table 'reviews' from  'reviews.json'...")
            for item in data:
                cursor.execute(f"""
                INSERT INTO reviews (id, artist, album, rating_data) VALUES(
                    '{item["video_id"]}',
                    '{item["artist"]}',
                    '{item["album"]}',
                    '{json.dumps(item["rating_data"])}'
                )
                """)
            conn.commit()
        cursor.close()
    raise SystemExit(0)

@click.command()
@click.option("--no-pull", is_flag=True, help="Do not perform a playlist pull from YouTube")
@click.option("--create-db", is_flag=True, help="Create a database with fetched JSON data")
def main(no_pull, create_db):
    if not no_pull:
        fetch_playlist_data()
    if create_db:
        create_database()

if __name__ == "__main__":
    main()
    logfile.close()