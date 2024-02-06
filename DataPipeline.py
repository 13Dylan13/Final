from database import database_connection
from database import select_all
from app import getoutcomes
from app import getVTM
import datetime
import os
import logging

logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG)
logging.info(f'{datetime.datetime.now()} - Start pipeline...')
subday = 15 #number of historic extracts to check for


cwd = os.chdir(r'C:/Users/DR2806/OneDrive - Capita Plc/P Drive/DataScience/Advanced/final')
logging.info(f'{datetime.datetime.now()} - Reading database for loaded filenames')
db = database_connection() 
checkfile = select_all(db, 'extractedfiles')   
n=0
while n<subday:
    startdate = datetime.datetime.today() - datetime.timedelta(days=n)
    logging.info(f'{datetime.datetime.now()} - Process data to trigger CSAT survey')
    getoutcomes(db,startdate,checkfile)
    logging.info(f'{datetime.datetime.now()} - Process data for CSAT survey responses')
    getVTM(db,startdate,checkfile)
    n=n+1
db.close()
logging.info(f'{datetime.datetime.now()} - Closing Database connection')
