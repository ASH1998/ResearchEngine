import psycopg2
import pandas as pd
import json
from tqdm import tqdm

with open('config.json') as f:
    configDict = json.load(f)

with open('config_queries.json') as f2:
    configQueries = json.load(f2)

inserytQuery = configQueries['insertQuery']

user = configDict["user"]
password = configDict['password']
storage = configDict['storageip']
port = configDict['port']
database = configDict['database']

connection = psycopg2.connect(user = user,
                                  password = password,
                                  host = storage,
                                  port = port,
                                  database = database)

cur = connection.cursor()

df = pd.read_pickle('../data/arxiv-last50years-data.pickle')

for i in tqdm(df.index):
    cur.execute(inserytQuery, [str(df.id[i]), str(df.submitter[i]),
                               str(df.title[i]), 
                               str(df.categories[i]),
                               str(df.abstract[i]), str(df.update_date[i]), str(df.authors_parsed[i]), int(df.year[i]),
                               str(df.pdf_link[i]), str(df.pages[i]), 'pipeline'
                              
                            ])
    
    if i%100==0:
        connection.commit()
    
connection.commit()

cur.close()
connection.close()