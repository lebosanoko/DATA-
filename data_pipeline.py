from google.colab import files
uploaded = files.upload()
conn = sqlite3.connect('chinook.db')

cur = conn.cursor()

import pandas as pd
from pyspark.sql import SparkSession
df_customers = pd.read_sql_query('SELECT * FROM customers', conn)
df_customers.to_csv("customers.csv", index=False)
customers_csv_df = spark.read.csv("customers.csv", header=True)
# Initialisation de SparkSession
spark = SparkSession.builder \
    .appName("Exo ETL") \
    .getOrCreate()

customers_csv_df = spark.read.csv("customers.csv", header=True)
customers_sorted = customers_csv_df.orderBy("LastName")
top_countries = customers_spark_df.groupBy("Country").count().orderBy("count", ascending=False).limit(10)
#top_countries.write.parquet("top_countries.parquet")
playlists_df = pd.read_sql_query("SELECT * FROM playlists", conn)
playlist_track_df = pd.read_sql_query("SELECT * FROM playlist_track", conn)
tracks_df = pd.read_sql_query("SELECT * FROM tracks", conn)
# Jointure des tables
combined_df = playlist_track_df.merge(playlists_df, on="PlaylistId").merge(tracks_df, on="TrackId")

final_table = combined_df[["PlaylistId", "TrackId", "Name_x", "Name_y", "Composer"]]

final_table.columns = ["PlaylistID", "TrackID", "Name (playlist)", "Name (tracks)", "Composer"]
# Connexion à la base de données SQLite
conn_sqlite = sqlite3.connect('chinookl.db')

# Écriture du DataFrame dans la base de données SQLite
final_table.to_sql("playlist_track_info", conn_sqlite, if_exists="append", index=False)

customers_csv_df.show()
customers_sorted.show()
top_countries.show()
print(final_table)