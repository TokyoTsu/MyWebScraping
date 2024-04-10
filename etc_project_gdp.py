import requests
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
csv_file = '/home/project/Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
db_name = 'World_Economies.db'
table_attribs =["Country", "GDP_USD_million"]
log_file = 'etl_project_log.txt'

# Code for ETL operations on Country-GDP data
# Importing the required libraries
def extract(url, table_attribs):
   html_page = requests.get(url).text
   data = BeautifulSoup(html_page, 'html.parser')
   df = pd.DataFrame(columns=table_attribs)
   tables = data.find_all('tbody')
   rows = tables[2].find_all('tr')
   for row in rows:
    col = row.find_all('td')
    if len(col)!=0:
        data_dict = {"Country": col[1].contents[0],
                    "GDP_USD_million": col[2].contents[0]}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True)            
    return df

def transform(df):
   gdp_list = df["GDP_USD_million"].tolist()
   gdp_list = [float("".join(x.split(','))) for x in gdp_list]
   gdp_list = [np.round(x/1000,2) for x in gdp_list]

   df['GDP_USD_million'] = gdp_list
   df = df.rename(columns = {"GDP_USD_million":"GDP_USD_billion"})
   return df

def load_to_csv(df, csv_path):
   df.to_csv(csv_file)

def load_to_db(df, sql_connection, table_name):
   df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
   print(query_statement)
   query_output = pd.read_sql(query_statement, sql_connection)
   print(query_output)

def log_progress(message):
    timestamp_format= '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


# Execute tasks
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
print(df.head(20))

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_file)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billion >=100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
