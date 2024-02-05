# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 07:06:16 2024

@author: DR2806
"""
import os
from zipfile import ZipFile 
import pandas as pd
import datetime
from database import select_all
from database import insert_extracted_filename


def getoutcomes(db,startdate,checkfile):
    password = 'ZipItself2' #I might add this to a config file
    cwd = os.chdir(r'C:\Users\DR2806\Downloads')
    filename = get_OutcomeFile_name(startdate,'.csv.zip')
    file = os.path.isfile(filename) #if the file exists set to true
    if file:
        check = ''
        c = 0
        #check that database to see if the file has been processed
        while c < len(checkfile):
            if checkfile[c][0] == filename[0:37]:
                check = 'loaded'
            else:
                pass
            c=c+1
            
        if check == 'loaded':
            print(filename, "already loaded")
        else:
            cwd = os.chdir(r'C:\Users\DR2806\Downloads')
            with ZipFile(filename, "r") as zip:
                zip.extractall(path="uncompressed", pwd=password.encode("utf-8"))
            #open the folder with the uncompressed data
            cwd = os.chdir(r'C:\Users\DR2806\Downloads\uncompressed\download\tfl\in')
            filename = get_OutcomeFile_name(startdate,'.csv')
            outcomes = pd.read_csv(filename, low_memory=False)
            #delete the umcompressed file
            os.remove(filename)
            #Excel process for initial pipeline
            nstartdate = startdate - datetime.timedelta(days=1)
            phase1_pipeline_save_Excel(outcomes,nstartdate)
            #To be replaced with a database save
            #update database
            data = [filename[0:37],datetime.datetime.today().strftime('%Y-%m-%d')]
            insert_extracted_filename(db, 'extractedfiles', data)
    else:
        pass

def get_OutcomeFile_name(date,filetype):
    startfilename = "CapitaTFL-CampaignOutcomes_"
    month=date.month
    year=date.year
    day=date.day
        
    if month < 10:
        if day > 9:
            filename = str(startfilename+str(day)+'-'+'0'+str(month)+'-'+str(year)+filetype)
        else:
            filename = str(startfilename+'0'+str(day)+'-'+'0'+str(month)+'-'+str(year)+filetype)
    else:
        if day > 9:
            filename = str(startfilename+str(day)+'-'+str(month)+'-'+str(year)+filetype)
        else:
            filename = str(startfilename+'0'+str(day)+'-'+str(month)+'-'+str(year)+filetype)
    return (filename)

def get_sent_file_name(date,filetype, startfilename):
    month=date.month
    year=date.year
    day=date.day
        
    if month < 10:
        if day > 9:
            filename = (startfilename+str(day)+'0'+str(month)+str(year)+filetype)
        else:
            filename = (startfilename+'0'+str(day)+'0'+str(month)+str(year)+filetype)
    else:
        if day > 9:
            filename = (startfilename+str(day)+str(month)+str(year)+filetype)
        else:
            filename = (startfilename+'0'+str(day)+str(month)+str(year)+filetype)
    return (filename)

   
 
    
def phase1_pipeline_save_Excel(outcomes,startdate):
    cwd = os.chdir(r'C:\Users\DR2806\Desktop\New CSAT Tracker\Inbox')
    filename = get_sent_file_name(startdate, '.xls', "TFL(Sent) ")
    outcomes.to_excel(filename, sheet_name=filename[0:18],index=False)#, index=False)
    
def phase1_pipeline_saveVTM_Excel(outcomes,startdate):
    cwd = os.chdir(r'C:\Users\DR2806\Desktop\New CSAT Tracker\Inbox')
    filename = get_sent_file_name(startdate, '.xls', "TFL(VTM) ")
    outcomes.to_excel(filename, sheet_name=filename[0:18],index=False)#, index=False)
    
def get_VTM_file_name(date,filetype, startfilename):
    month=date.month
    year=date.year
    day=date.day
        
    if month < 10:
        if day > 9:
            filename = (startfilename+str(day)+'0'+str(month)+str(year)+filetype)
        else:
            filename = (startfilename+'0'+str(day)+'0'+str(month)+str(year)+filetype)
    else:
        if day > 9:
            filename = (startfilename+str(day)+str(month)+str(year)+filetype)
        else:
            filename = (startfilename+'0'+str(day)+str(month)+str(year)+filetype)
    return (filename)
   
def get_initialVTMFile_name(date,filetype):
    startfilename = "owner-9823-VTM_10838-TFL_Customer_Survey-"
    month=date.month
    year=date.year
    day=date.day
        
    if month < 10:
        if day > 9:
            filename = (startfilename+str(year)+'0'+str(month)+str(day)+filetype)
        else:
            filename = (startfilename+str(year)+'0'+str(month)+'0'+str(day)+filetype)
    else:
        if day > 9:
            filename = (startfilename+str(year)+str(month)+str(day)+filetype)
        else:
            filename = (startfilename+str(year)+str(month)+'0'+str(day)+filetype)
    return (filename)   

def getVTM(db,startdate,checkfile):
    password = 'ZipItself2' #I might add this to a config file
    cwd = os.chdir(r'C:\Users\DR2806\Downloads')
    filename = get_initialVTMFile_name(startdate, '.csv.zip')
    file = os.path.isfile(filename)
    if file:
        check = ''
        c = 0
        #check that database to see if the file has been processed
        while c < len(checkfile):
            if checkfile[c][0] == filename[0:49]:
                check = 'loaded'
            else:
                pass
            c=c+1
            
        if check == 'loaded':
            print(filename, "already loaded")
        else:
            cwd = os.chdir(r'C:\Users\DR2806\Downloads')
            with ZipFile(filename, "r") as zip:
                zip.extractall(path="uncompressed", pwd=password.encode("utf-8"))
            #open the folder with the uncompressed data
            cwd = os.chdir(r'C:\Users\DR2806\Downloads\uncompressed\download\tfl\in')
            filename = get_initialVTMFile_name(startdate,'.csv')
            outcomes = pd.read_csv(filename, low_memory=False)
            #delete the umcompressed file
            os.remove(filename)
            #Excel process for initial pipeline
            nstartdate = startdate - datetime.timedelta(days=1)
            phase1_pipeline_saveVTM_Excel(outcomes,nstartdate)
            #To be replaced with a database save
            #update database
            data = [filename[0:49],datetime.datetime.today().strftime('%Y-%m-%d')]
            insert_extracted_filename(db, 'extractedfiles', data)
    else:
        pass
        

