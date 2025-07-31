# this is the script to ingest the data into the database 
# we will use sqlalchemy to create the connection to the database
# the data is for the entire year 2024

import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Create a 'logs' folder if it doesn't exist already
# This is where we'll save our log files
os.makedirs('logs', exist_ok=True)


# This tells the program how to write log messages
logging.basicConfig(
    filename="logs/ingestion_db.log",  # Log file will be saved here
    level=logging.DEBUG,               # Log all types of messages (info, warnings, errors)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format: timestamp - level - message
    filemode="a"                      # 'a' means append - add new logs to existing file instead of overwriting
)


# Creating a connection to SQLite database
# This will create a file called 'inventory.db' in the current folder
engine = create_engine('sqlite:///inventory.db')


# Function to save a dataframe (table) into the database and fetch only useful data through SQL query
def ingest_db(df, table_name, engine):
  '''
  This function takes a dataframe and saves it as a table in the database
  df: the data we want to save
  table_name: what we want to call the table in the database
  engine: the database connection
  '''
  # Save the dataframe to database, replace existing table if it exists
  df.to_sql(name=table_name, con=engine, if_exists='replace', index=False) 



# Main function that reads all CSV files and saves them to database
def load_raw_data():
  '''
  This function:
  1. Looks for all CSV files in the data/data folder
  2. Reads each CSV file
  3. Saves each file as a separate table in the database
  4. Logs what it's doing
  '''
  
  start = time.time()
  logging.info('---------Ingestion Started---------') 

  # Path where our CSV files are stored
  data_path = 'data/data'
  
  # Looks through each file in the data folder
  for file in os.listdir(data_path):
    # Checks if the file is a CSV file
    if '.csv' in file:
      # Reads the CSV file into a dataframe
      df = pd.read_csv(f'{data_path}/{file}')
      logging.info(f'Ingesting {file} in db')  # Logs which file we're processing
      ingest_db(df, file[:-4], engine) # file[:-4] removes '.csv' from the filename
  
  end = time.time()
  logging.info(f'Ingestion Time: {(end-start)/60} minutes')
  logging.info('---------Ingestion Completed---------')

if __name__ == '__main__':
  load_raw_data()
