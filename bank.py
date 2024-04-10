import requests
import sqlite3
import pandas as pd
import numpy as np
import csv
from datetime import datetime
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_file = '/home/project/exchange_rate.csv'
csv_load = 'Largest_banks_data.csv'
table_name = 'Largest_banks'
db_name = 'Banks.db'
table_attribs =["Name", "MC_USD_Billion"]
log_file = 'code_log.txt'

def log_progress(message):
    # Year-Monthname-Day-Hour-Minute-Second 
    timestamp_format= '%Y-%m-%d-%H:%M:%S' 
    # get current timestamp
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ':' + message + '\n') 

def extract(url, table_attribs):
   html_page = requests.get(url).text
   data = BeautifulSoup(html_page, 'html.parser')
   df = pd.DataFrame(columns=table_attribs)
   tables = data.find_all('tbody')
   rows = tables[0].find_all('tr')
   for row in rows:
     col = row.find_all('td')
     print(col)
     if len(col)!=0:
        title = col.find('a')
        if title is not None and '-' not in title.text:
          data_dict = {"Bank name": title.text, "MC_USD_Billion": col[2].contents[0]}
          df1 = pd.DataFrame(data_dict, index=[0])
          df = pd.concat([df, df1], ignore_index=True)  
   print(df)          
   return df

def transform(csv_file, df):
    dataframe = pd.read_csv(csv_file)
    dict = dataframe.set_index('Currency').to_dict()['Rate']
    df1 = pd.DataFrame(columns=["MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"])
    gbp_rate = dict["GBP"]
    usd = [x.strip() for x in df['MC_USD_Billion']]
    usd_fl = [float(x) for x in usd]
    eur_rate = dict["EUR"]
    inr_rate = dict["INR"]
    df1['MC_GBP_Billion'] = [np.round(x* gbp_rate, 2) for x in usd_fl] 
    df1['MC_EUR_Billion'] = [np.round(x*eur_rate, 2) for x in usd_fl]
    df1['MC_INR_Billion'] = [np.round(x*inr_rate, 2) for x in usd_fl]
    df = pd.concat([df, df1])
    print(df['MC_EUR_Billion'])
    return df

def load_to_csv(df, csv_load):
   df.to_csv(csv_load)

def load_to_db(df, sql_connection, table_name):
   df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
   print(query_statement)
   query_output = pd.read_sql(query_statement, sql_connection)
   print(query_output)


# Execute tasks
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(csv_file, df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_load)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
