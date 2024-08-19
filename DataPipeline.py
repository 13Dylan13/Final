from database import database_connection
from database import select_all
from app import getoutcomes
from app import getVTM
from app import table_create
import datetime
import os
import logging
import pandas as pd


logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG)
logging.info(f'{datetime.datetime.now()} - Start pipeline...')
subday = 28 #number of historic extracts to check for
new_setup = 0 #set to 1 to create database tables

cwd = os.chdir(r'C:/Users/DR2806/OneDrive - Capita Plc/P Drive/DataScience/Advanced/final')
logging.info(f'{datetime.datetime.now()} - Reading database for loaded filenames')
db = database_connection() 

if new_setup == 1:
    logging.info(f'{datetime.datetime.now()} - checking and creating tables')
    table_create(db)
else:
    pass

checkfile = select_all(db, 'extractedfiles')   
n=0
while n<subday:
    startdate = datetime.datetime.today() - datetime.timedelta(days=n)
    logging.info(f'{datetime.datetime.now()} - Process data to trigger CSAT survey')
    getoutcomes(db,startdate,checkfile)
    logging.info(f'{datetime.datetime.now()} - Process data for CSAT survey responses')
    getVTM(db,startdate,checkfile)
    n=n+1
    #insert_extracted_filename(db, 'vtmcount', data)

vtmlist = select_all(db, 'vtm_vol')   
vtm_file = pd.DataFrame(vtmlist)
outputlist = select_all(db, 'outcomes_vol')
output_file = pd.DataFrame(outputlist)
cwd = os.chdir(r'C:/Users/DR2806/OneDrive - Capita Plc/P Drive/DataScience/Advanced/final')
vtm_file.to_csv('vtm_volumes_processed.csv', header = False, index = False)
output_file.to_csv('output_volumes_processed.csv', header = False, index = False)   
#print(vtmlist)
#db.executescript('drop table if exists outcomes_vol;')
#db.executescript('drop table if exists vtm_vol;')

db.close()
logging.info(f'{datetime.datetime.now()} - Closing Database connection')


