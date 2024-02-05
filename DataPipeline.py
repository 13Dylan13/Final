from database import database_connection
from database import select_all
from app import getoutcomes
from app import getVTM
import datetime
import os


subday = 15 #number of historic extracts to check for

cwd = os.chdir(r'C:/Users/DR2806/OneDrive - Capita Plc/P Drive/DataScience/Advanced/final')
db = database_connection() 
checkfile = select_all(db, 'extractedfiles')   
#could add loop for historic days here
n=0
while n<subday:
    startdate = datetime.datetime.today() - datetime.timedelta(days=n)
    getoutcomes(db,startdate,checkfile)
    getVTM(db,startdate,checkfile)
    n=n+1
db.close()


