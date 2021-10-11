import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, time
import datetime
import _sqlite3


# DATABASE_LOCATION = "sqllite:///my_played_tracks.sqlite"
USER_ID = "deus.caj"
TOKEN ="BQBo1izsaYO8ZZqIUbEKJ-wo7PWdFqKWY2wAteVMSKexbCApeBcS31A7YtbRIVmBi4Qi1SzXY4OhoW0Ytp48foHWRXB-nbcn0cnolh60q57uFzSVsqWIr1dPZNobMxya84yVhdyaLjeYp_3N"


def check_if_valid_data(df:pd.DataFrame) -> bool:
        if df.empty:
            print("No songs downloaded. Finishing execution")
            return False

        if pd.Series(df['played_at']).is_unique:
            pass
        else:
            raise Exception("Primary Key check is violated") 
        if df.isnull().values.any():
            raise Exception("Null Values found")   
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)


        timestamps  = df["timestamp"].tolist() #toList convert data frame series ke list
        for timestamp in timestamps:
            if datetime.datetime.strptime(timestamp,"%Y-%m-%d") != yesterday:
                raise Exception ("There are at least one data that not come within 24 hours")
       
       
        
                
        return True
if __name__ == "__main__":
    headers = {
        "Accept": "application/json",
        "Content-Type" : "application/json",
        "Authorization": "Bearer {token}".format(token = TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

  
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after{time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

song_names      = []
artist_names    = []
played_at_list  = []
timestamps      = []

for song in data["items"]:  
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])


song_dict = {
    "song_name"     :song_names,
    "artist_name"   : artist_names,
    "played_at"     :played_at_list,
    "timestamp"     : timestamps

}    

song_df = pd.DataFrame(song_dict, columns= ["song_name","artist_name","played_at","timestamp"])

if check_if_valid_data(song_df):
    print("Data valid,proceed to Load ")

print(song_df)  

