#Workshop etl 02

import pandas as pd
import logging
import json
import mysql.connector
from drive_conn import upload_csv

def conn_mysql():
    with open('/home/vinke1302/Apache/db_config.json') as f:
        dbfile = json.load(f)
    
    connection = mysql.connector.connect(
        host=dbfile["host"],
        user=dbfile["user"],
        password=dbfile["password"],
        database=dbfile["database"]
    )
    
    print("Database connection ok")
    return connection

def read_csv():
    csv_spotify = "/home/vinke1302/Apache/spotify_dataset.csv"
    df_spotify = pd.read_csv(csv_spotify, delimiter=',')
    logging.info("Spotify CSV readed sucesfully")

    return df_spotify.to_json(orient='records')

def transform_csv(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_csv")
    json_data = json.loads(str_data)
    spotify = pd.json_normalize(data=json_data)

    def transform_duration(duration_ms):
        minutes = duration_ms // 60000
        seconds = (duration_ms % 60000) // 1000
        return f"{minutes}:{seconds}"
    spotify['duration_ms'] = spotify['duration_ms'].apply(transform_duration)
    spotify.rename(columns={'duration_ms': 'duration_min_sec'}, inplace=True)

    def categorize_popularity(popularity):
        if popularity <= 25:
            return "0-25"
        elif popularity <= 50:
            return "25-50"
        elif popularity <= 75:
            return "50-75"
        else:
            return "75-100"
    spotify['popularity_categorize'] = spotify['popularity'].apply(categorize_popularity)

    spotify.drop(columns=["mode"], inplace=True)
    spotify.drop(columns=["Unnamed: 0"], inplace=True)

    valence_bins = [0, 0.25, 0.50, 0.75, 1.0]
    valence_labels = ["0.0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.0"]
    spotify['valence_category'] = pd.cut(spotify['valence'], bins=valence_bins, labels=valence_labels)

    speechiness_bins = [0, 0.33, 0.66, 1.0]
    speechiness_labels = ["Music", "Mixed", "Speech"]
    spotify['speechiness_category'] = pd.cut(spotify['speechiness'], bins=speechiness_bins, labels=speechiness_labels)

    danceability_bins = [0, 0.25, 0.50, 0.75, 1.0]
    danceability_labels = ["0.0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.0"]
    spotify['danceability_category'] = pd.cut(spotify['danceability'], bins=danceability_bins, labels=danceability_labels)

    energy_bins = [0, 0.25, 0.50, 0.75, 1.0]
    energy_labels = ["0.0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.0"]
    spotify['energy_category'] = pd.cut(spotify['energy'], bins=energy_bins, labels=energy_labels)

    logging.info("The Spotify CSV has done the transformations")
    spotify_result = spotify.to_json(orient='records')
    return spotify_result

def read_db():
    try:
        connection = conn_mysql() 
        query = f"SELECT * FROM grammy_awards"
        df_grammyAwards = pd.read_sql(query, connection)

        connection.close()

        logging.info("Database of grammy awards read successfully")
        return df_grammyAwards.to_json(orient='records')

    except Exception as e:
        print("Error:", str(e))
        return None

def transform_db(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids = "read_db")
    json_data = json.loads(str_data)
    df_grammys = pd.json_normalize(data=json_data)
    logging.info("DB has started the transformation proccess")

    def drop_columns(dataframe, columns):
        for column in columns:
            if column in dataframe.columns:
                dataframe.drop(columns=[column], inplace=True)
    drop_columns(df_grammys, ['published_at', 'updated_at'])

    def rename_category_column(dataframe):
        if 'category' in dataframe.columns:
            dataframe.rename(columns={'category': 'grammy_category'}, inplace=True)
    rename_category_column(df_grammys)

    def remove_nulls(dataframe, column_name):
        dataframe.dropna(subset=[column_name], inplace=True)
    remove_nulls(df_grammys, 'nominee')

    logging.info("The grammy awards Excel has done the transformations")
    return df_grammys.to_json(orient='records')

def merge(**kwargs):
    ti = kwargs["ti"]

    logging.info("Spotify is entering the merge function")
    str_data_spotify = ti.xcom_pull(task_ids="transform_csv")
    spotify_data = json.loads(str_data_spotify)
    spotify_df = pd.json_normalize(data=spotify_data)

    logging.info("Grammys is entering the merge function")
    str_data_grammys = ti.xcom_pull(task_ids="transform_db")
    grammys_data = json.loads(str_data_grammys)
    grammys_df = pd.json_normalize(data=grammys_data)

    merged_df = spotify_df.merge(grammys_df, how='left', left_on='track_name', right_on='nominee')

    def organizing_columns(df):
        columns = ['track_id', 'artists', 'album_name', 'track_name',
                   'popularity_categorize', 'duration_min_sec',
                    'explicit', 'danceability_category', 'energy_category',
                   'loudness', 'speechiness_category', 'acousticness',
                   'instrumentalness', 'liveness', 'valence_category', 'tempo', 'time_signature',
                   'track_genre', 'grammy_category', 'nominee', 'winner']
        df = df[columns]
        return df

    def fill_na_after_merge(df):
        df.fillna({'winner': 0}, inplace=True)
        return df

    # Llama a las funciones auxiliares para organizar las columnas y llenar valores nulos
    merged_df = organizing_columns(merged_df)
    merged_df = fill_na_after_merge(merged_df)

    logging.info("Data is ready for deployment")
    return merged_df.to_json(orient='records')

def load(**kwargs):
    logging.info("Load process on")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="merge")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    connection = conn_mysql()

    try:
        cursor = connection.cursor()

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS music (
                track_id VARCHAR(255),
                artists VARCHAR(600),
                album_name VARCHAR(255),
                track_name VARCHAR(600),
                popularity_categorize VARCHAR(10),
                duration_min_sec VARCHAR(10),
                explicit TINYINT(1),
                danceability_category VARCHAR(10),
                energy_category VARCHAR(10),
                loudness FLOAT,
                speechiness_category VARCHAR(10),
                acousticness FLOAT,
                instrumentalness FLOAT,
                liveness FLOAT,
                valence_category VARCHAR(10),
                tempo FLOAT,
                time_signature INT,
                track_genre VARCHAR(255),
                grammy_category VARCHAR(255),
                nominee VARCHAR(255),
                winner TINYINT(1)
            );
        """
        
        cursor.execute(create_table_query)
        logging.info("Created Table")

        data_to_insert = [tuple(row) for row in df.values]
        insert_query = f"INSERT INTO music VALUES ({', '.join(['%s'] * len(df.columns))})"

        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        logging.info("Data Inserted")

        try:
            query = "SELECT * FROM music"
            df_csv = pd.read_sql(query, connection)
            csv_filename = "/home/vinke1302/Apache/music_data.csv"
            df_csv.to_csv(csv_filename, index=False)
            logging.info("Data saved to CSV file")

            return df_csv.to_json(orient='records')
            
        except Exception as e:
            print("Error:", str(e))
            return None

    except Exception as e:
        print("Error:", str(e))
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def store(**kwargs):
    logging.info("Starting store process")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="load")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    # Subir el archivo CSV a Google Drive
    upload_csv("/home/vinke1302/Apache/music_data.csv", '1C08_wtItOUcDjr045zd6_lg_Ge4iAQOd')  

    logging.info("Data has completed the store process")









