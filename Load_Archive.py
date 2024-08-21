#This code will load the historic CSAT responses from the Excel archieve
#This data will be loaded into the TfLcsat.db
#for pre-live I could include an update of this archieve from the pipeline automation


from database import create_vtm_table
from database import database_connection
from database import create_outcomes_table
from database import create_questions
import pandas as pd
import datetime
import os
import logging
import io


logging.basicConfig(filename='pipeline.log', encoding='utf-8', level=logging.DEBUG)
cwd = os.chdir(r'C:/Users/DR2806/OneDrive - Capita Plc/P Drive/DataScience/Advanced/final')
db = database_connection() 
logging.info(f'{datetime.datetime.now()} - TfLcsat database open')
cwd = os.chdir(r'C:/Users/DR2806/Desktop/New CSAT Tracker/Databases')
logging.info(f'{datetime.datetime.now()} - Database path accessed')

#create tables here
new_setup = 1 #set to 1 to create database tables
if new_setup == 1:
    logging.info(f'{datetime.datetime.now()} - checking and creating tables')   
    #Create tables
    create_vtm_table(db)
    create_outcomes_table(db)
    create_questions(db)
    questions = pd.read_csv('questions.csv', low_memory=False)
    questions.to_sql('questions', db, if_exists='replace', index=False)
else:
    pass

#Insert data from csv if available
vtmHist = pd.read_csv('vtmHist.csv', low_memory=False)
outcomesHist = pd.read_csv('outcomeH.csv', low_memory=False)
logging.info(f'{datetime.datetime.now()} - History Loaded')
vtmHist['account_name'] = 'TransportforLondon' 
vtmHist['call_id'] = ''
vtmHist = vtmHist.rename(columns={'Q1. Did we resolve your enquiry today? ': 'answer_1',
                                  'We  would really like to understand your experiences fully. Please use the space below to tell us the reason for your scores:': 'answer_2',
                                  'AgentID': 'agent_id', 
                                  'SURVEY DATE': 'call_date', 
                                  'Ref': 'call_id',
                                  'VoiceSageCalloutID': 'voicesagecalloutid'})
vtmHist = vtmHist[['voicesagecalloutid','account_name','call_id','call_date','agent_id','answer_1','answer_2']]
vtmHist = vtmHist.dropna()
vtmHist.to_sql('vtm_responses', db, if_exists='replace', index=False)
logging.info(f'{datetime.datetime.now()} - vtm added to database')
outcomesHist['account_name'] = 'TransportforLondon' 
outcomesHist['start_time'] = ''
outcomesHist['stop_time'] = ''
outcomesHist = outcomesHist.rename(columns={'Ref': 'call_id', 'CallTime': 'call_time'})
outcomesHist = outcomesHist[['account_name','call_id','call_time','start_time','stop_time']]
outcomesHist = outcomesHist.dropna()
outcomesHist.to_sql('outcomes', db, if_exists='replace', index=False)
logging.info(f'{datetime.datetime.now()} - outcomes added to database')


db.close()
logging.info(f'{datetime.datetime.now()} - Closed Database connection')


