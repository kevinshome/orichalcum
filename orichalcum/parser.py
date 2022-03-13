from typing import Union, Dict, TextIO, Optional

problem_titles = {
    "kendrick lamar untitled unmastered. review | 03.06.16.mov": {
        "artist": "Kendrick Lamar",
        "album": "untitled unmastered."
    },
    "CX KiDTRONiK: KRAK ATTACK 2: THE BALLAD OF ELLI SKIFF ALBUM REVIEW": {
        "artist": "CX KiDTRONiK",
        "album": "KRAK ATTACK 2: THE BALLAD OF ELLI SKIFF"
    },
    "An Evening with Silk Sonic ALBUM REVIEW": {
        "artist": "Silk Sonic",
        "album": "An Evening with Silk Sonic"
    },
    "Star Wars Head Space VARIOUS ARTISTS COMPILATION REVIEW": {
        "artist": "Various Artists",
        "album": "Star Wars Head Space"
    },
    "A-1- After School Special ALBUM REVIEW": {
        "artist": "A-1",
        "album": "After School Special"
    }
}

def parse_description(description: str) -> Dict[str, str]:
    '''Parse description and return dictionary with necessary values.'''
    vals = {}
    for line in description.split('\n'):
        if line.startswith("FAV TRACKS"):
            vals["fav_tracks"] = line.split(':')[1].strip()
        elif line.startswith("LEAST FAV TRACK"):
            try:
                vals["least_fav_tracks"] = line.split(':')[1].strip()
            except IndexError:
                vals["least_fav_tracks"] = line.split('—')[1].strip()
        elif "/10" in line and line[0].isdigit():
            line = line.split()[0]
            vals["rating"] = line.replace("/10", "")
    return vals

def create_video_object(api_return_data: dict, log_file: TextIO) -> Optional[Dict[str, Union[str, Dict[str, str]]]]:
    '''Create an object with only the data we need from a chunk of API response data.'''
    title: str = api_return_data["snippet"]["title"]
    description: str = api_return_data["snippet"]["description"]
    video_id: str = api_return_data["snippet"]["resourceId"]["videoId"]
    artist: str = title.split('- ')[0].strip()

    try:
        album: str = '-'.join(title.split('- ')[1:])\
                        .strip('-')\
                        .replace("Album Review", "")\
                        .replace("Review", "")\
                        .split("(QUICK")[0]\
                        .split("ALBUM")[0]\
                        .split("/EP")[0]\
                        .split("EP/")[0]\
                        .split("MIXTAPE")[0]\
                        .split("ft.")[0]\
                        .strip()
    except IndexError:
        print("Error handling the following video title:", title, file=log_file)
        print("videoId:", video_id, file=log_file)
        print("===========================", file=log_file)
        return None
    
    if title in problem_titles: # our parser has trouble properly handling some titles, so we need to give it a tiny hand sometimes
        artist = problem_titles[title]["artist"]
        album = problem_titles[title]["album"]

    return {
        "artist": artist.replace("'", "''"),
        "album": album.replace("'", "''"),
        "video_id": video_id,
        "rating_data": parse_description(description.replace("'", "''"))
    }