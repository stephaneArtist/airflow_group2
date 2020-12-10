import time  #pour créer une playlist horodatée
from datetime import datetime
from random import shuffle #pour attribuer un classement aléatoire aux morceaux
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd


def defDate(date):
    try:
        return datetime.strptime(date,"%Y-%m-%d")
    except:
        try:
            return datetime.strptime(date,"%Y-%m")
        except:
            try :
                return datetime.strptime(date,"%Y")
            except:
                return datetime.date(3030,12,12)
            


def main():
    df = pd.DataFrame(columns=["Current Day",'Playlist Country','Playlist Name', 'PlayList Description', 'Playlist Total Track','Track add on Spotify','Album Type','Album Name',"Album Artist Name","Album Artist Type","Album Artist id","Album Release Date","Album Total Track","Artist Track Name","Artist Track id","Artist Track type","Track Disc Number","Track Duration ms","Track id","Track Name","Track Populary","Track Number","Track Type"])
    dc={}
    L = []
    Country ="FR"
    CurrentDate = datetime.today()
    username="Automatisation.infrastructure.de.données.g2"
    clientId= "8ea50f22dacf46989a0c3c04c8599b76"
    clientSecret="0d5b0ceb29314f3fb504e921ee2be85a"

    sp = spotipy.Spotify(auth_manager=spotipy.oauth2.SpotifyClientCredentials(client_id=clientId, client_secret=clientSecret))

    Manybest_playlists = sp.featured_playlists(country=Country)

    for OneBest_Playlist in Manybest_playlists["playlists"]["items"] :       
            # Obtenez tous les détails des pistes et des épisodes d'une playlist.
            playlists = sp.playlist_items( OneBest_Playlist["id"] )
            for track in playlists["items"]:
                    try:
                            listArtistNameOfAlbum, listArtistIdOfAlbum, listArtistTypeOfAlbum = [],[],[]
                            listArtistNameOfTrack, listArtistIdOfTrack, listArtistTypeOfTrack = [], [], []
                            ## liste des artistes qui ont participé a l'album du track
                            for ArtistOfAlbum in track["track"]["album"]["artists"]:
                                    listArtistNameOfAlbum.append(ArtistOfAlbum["name"])
                                    listArtistIdOfAlbum.append(ArtistOfAlbum["id"])
                                    listArtistTypeOfAlbum.append(ArtistOfAlbum["type"])
                            ## liste des artiste qui ont participé au track
                            for ArtistOfTrack in track["track"]["artists"]:
                                    listArtistNameOfTrack.append(ArtistOfTrack["name"])
                                    listArtistIdOfTrack.append(ArtistOfTrack["id"]) 
                                    listArtistTypeOfTrack.append(ArtistOfTrack["type"])
                            new_row = {
                            "Current Day":CurrentDate,
                            'Playlist Country':Country,
                            'Playlist Name':OneBest_Playlist["name"],
                            'PlayList Description':OneBest_Playlist["description"],
                            'Playlist Total Track':OneBest_Playlist["tracks"]["total"],
                            'Track add on Spotify':datetime.strptime(track['added_at'],"%Y-%m-%dT%H:%M:%SZ"),
                            'Album Type':track["track"]["album"]["album_type"],
                            'Album Name':track["track"]["album"]["name"],
                            ## liste des artistes qui ont participé a l'album du track
                            "Album Artist Name": listArtistNameOfAlbum ,
                            "Album Artist Type":listArtistTypeOfAlbum,
                            "Album Artist id":listArtistIdOfAlbum ,##end
                            "Album Release Date":defDate(track["track"]["album"]["release_date"]),
                            "Album Total Track":track["track"]["album"]["total_tracks"],
                            ## liste des artiste qui ont participé au track
                            "Artist Track Name":listArtistNameOfTrack,
                            "Artist Track id": listArtistIdOfTrack, 
                            "Artist Track type":listArtistTypeOfTrack,##end
                            "Track Disc Number":track["track"]["disc_number"],
                            "Track Duration ms":track["track"]["duration_ms"],
                            "Track id":track["track"]["id"],
                            "Track Name":track["track"]["name"],
                            "Track Populary":track["track"]["popularity"],
                            "Track Number":track["track"]["track_number"],
                            "Track Type": track["track"]["type"]
                            }
                            df = df.append(new_row,ignore_index=True)
                            L.append(new_row)                        
                    except:
                            a=0
    path = 'data_spotify_' + str(CurrentDate).split(" ")[0]+'.csv'
    df.to_csv(path,sep=";")

    destination = "/user/iabd2_group2/data"
    command = "hdfs dfs -copyFromLocal " + path + " " + destination
    print(command)
    os.system(command)