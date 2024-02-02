from database import database_connection
from database import select_all
from app import getoutcomes
from app import getVTM
import os


subday = 15 #number of historic extracts to check for

cwd = os.chdir(r'C:/Users/DR2806/OneDrive - Capita Plc/P Drive/DataScience/Advanced/final')
db = database_connection() 
checkfile = select_all(db, 'extractedfiles')   
getoutcomes(db,subday,checkfile)
getVTM(db,subday,checkfile)
db.close()



