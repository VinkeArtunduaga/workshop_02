import pandas as pd
import mysql.connector

# Ruta al archivo Excel
excel_file = "/home/vinke1302/Apache/the_grammy_awards.xlsx"

db_config = {
    "host": "localhost", 
    "user": "root",
    "password": "password",  
    "database": "ETL",  
}

try:
    
    df = pd.read_excel(excel_file)

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    table_name = "grammy_awards" 

    for _, row in df.iterrows():
        query = f"INSERT INTO {table_name} (year, title, published_at, updated_at, category, nominee, artist, winner) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (row["year"], row["title"], row["published_at"], row["updated_at"], row["category"], row["nominee"], row["artist"], row["winner"]))


    conn.commit()
    conn.close()

    print("Carga de datos exitosa.")

except Exception as e:
    print("Error:", str(e))
