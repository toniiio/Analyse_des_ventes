import requests
import pandas as pd
import psycopg2
import io
from sqlalchemy import create_engine


conn = psycopg2.connect(dbname="client_data", user="user", password="password", host="127.0.0.1", port="5432")
print("Connexion réussie !")
engine = create_engine("postgresql://user:password@localhost:5432/client_data")
conn.autocommit = True

cursor = conn.cursor()
cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'client_data'")
exists = cursor.fetchone()
if not exists:
    cursor.execute("CREATE DATABASE client_data;")
    print("Base de données 'client_data' créée avec succès !")
else:
    print("La base de données 'client_data' existe déjà.")

print("Base de données client_data créée avec succès !")

TABLES = {
    "store": "id SERIAL PRIMARY KEY, city TEXT NOT NULL,numberEmployees INTEGER NOT NULL",
    "sales": "id SERIAL PRIMARY KEY, date TEXT NOT NULL,refProduct TEXT NOT NULL,quantity INTEGER NOT NULL,store_id INTEGER NOT NULL,FOREIGN KEY (store_id) REFERENCES store(id)",
    "product": "id SERIAL PRIMARY KEY,name TEXT NOT NULL,refProduct TEXT NOT NULL,price INTEGER NOT NULL,stock INTEGER NOT NULL"
}

for table, schema in TABLES.items():
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema});")
print("Tables créées avec succès !")
urls = [
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=0&single=true&output=csv",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=714623615&single=true&output=csv",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=760830694&single=true&output=csv"
]

for i, url in enumerate(urls):
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(io.StringIO(response.text))
        print(df)
        table_name = f"table{i+1}"
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Données importées dans {table_name} !")
    else:
        print(f"Erreur de téléchargement pour {url} : {response.status_code}")

print("Processus terminé avec succès !")
cursor.close()
conn.close()

