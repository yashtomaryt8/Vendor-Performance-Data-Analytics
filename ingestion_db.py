import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True   # <-- important in Jupyter/when re-running scripts
)

print("Logging to:", os.path.abspath("logs/ingestion.log"))

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    """Ingest the dataframe into the database table"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"✅ {len(df)} rows ingested into '{table_name}'")

def load_raw_data():
    """Load CSVs as dataframes and ingest into db"""
    start = time.time()
    
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            df = pd.read_csv(file_path)
            logging.info(f"Ingesting {file} into DB")
            ingest_db(df, file[:-4], engine)
    
    end = time.time()
    total_time = (end - start) / 60
    logging.info("---------- Ingestion Complete ----------")
    logging.info(f"Total Time Taken: {total_time:.2f} minutes")

if __name__ == '__main__':
    load_raw_data()
    logging.shutdown()   # ensure logs are written
