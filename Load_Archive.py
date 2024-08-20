#This code will load the historic CSAT responses from the Excel archieve
#This data will be loaded into the TfLcsat.db
#for pre-live I could include an update of this archieve from the pipeline automation


from database import *
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
new_setup = 0 #set to 1 to create database tables
if new_setup == 1:
    logging.info(f'{datetime.datetime.now()} - checking and creating tables')   
    #Create tables
    create_vtm_table(db)
    create_outcomes_table(db)
    create_questions(db)
    #insert Question data
    
else:
    pass


    #Insert csv if available
    cwd = os.chdir(r'C:\Users\DR2806\Downloads')
    outcomes = pd.read_csv('outcomes.csv', low_memory=False)
    outcomes = outcomes[['date','volume']]
    outcomes = outcomes.dropna()
    outcomes.to_sql('outcomes_vol', db, if_exists='replace', index=False)
    outcomes = pd.read_csv('vtms.csv', low_memory=False)
    outcomes = outcomes[['date','volume']]
    outcomes = outcomes.dropna()
    outcomes.to_sql('vtm_vol', db, if_exists='replace', index=False)
    




#sent = pd.read_excel('TFL.xlsb', "Sent")
decrypted_workbook = io.BytesIO()
with open('TFL.xlsb', 'rb') as file:
    office_file = msoffcrypto.OfficeFile(file)
    office_file.load_key(password=XLpassword)
    office_file.decrypt(decrypted_workbook)

#sent = pd.read_excel(decrypted_workbook, sheet_name='Sent')
vtm = pd.read_excel(decrypted_workbook, sheet_name='VTM')
vtm['SURVEY DATE'] = pd.to_datetime(vtm['SURVEY DATE'], unit='d', origin='1899-12-30')
#sent = sent[['Ref','CallTime','CLI']]
vtm.rename(columns = {'Q1. Did we resolve your enquiry today? ':'resolved'}, inplace = True)
vtm.rename(columns = {'We  would really like to understand your experiences fully. Please use the space below to tell us the reason for your scores:': 'verbatim'}, inplace = True)
vtm.rename(columns = {'SURVEY DATE':'survey_date'}, inplace = True)
vtm.rename(columns = {'VoiceSageCalloutID':'surveyid'}, inplace = True)
vtm = vtm[['surveyid','resolved','verbatim','AgentID','survey_date']]
vtm['surveyid'] = vtm['surveyid'].str.replace(r'-', '', regex=True)

#DB
db = database_connection() 
create_vtm_table(db)
insert_vtm_archive(db, 'vtm_responses', vtm)
#create_sent_table(db)
#create_questions(db)

#insert_3Column_DB_table(db, 'sent', sent)
#n=0
#while n<subday:
#    startdate = datetime.datetime.today() - datetime.timedelta(days=n)
#    logging.info(f'{datetime.datetime.now()} - Process data to trigger CSAT survey')
#    getoutcomes(db,startdate,checkfile)
#    logging.info(f'{datetime.datetime.now()} - Process data for CSAT survey responses')
#    getVTM(db,startdate,checkfile)
#    n=n+1
db.close()
logging.info(f'{datetime.datetime.now()} - Closed Database connection')


