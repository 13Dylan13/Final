import sqlite3

def database_connection():
    db = sqlite3.connect('TfLcsat.db')
    create_extractedfiles_table(db)
    return db

def create_extractedfiles_table(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS extractedfiles(
            file_name TEXT NOT NULL PRIMARY KEY,
            date_processed DATETIME
        ); ''')
        
def create_vtm_vol_table(db):
    db.execute('''
               CREATE TABLE IF NOT EXISTS vtm_vol(
                   date DATETIME NOT NULL PRIMARY KEY,
                   volume TEXT
                   ); ''')

def create_outcomes_vol_table(db):
    db.execute('''
               CREATE TABLE IF NOT EXISTS outcomes_vol(
                   date DATETIME NOT NULL PRIMARY KEY,
                   volume TEXT
                   ); ''')        
        
def create_vtm_table(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS vtm_responses(
            filename TEXT,
            surveyid INTEGER NOT NULL PRIMARY KEY,
            call_id NUMBER,
            resolved TEXT,
            verbatim TEXT,
            AgentID TEXT,
            survey_date TIMESTAMP
        ); ''')

def create_questions(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS sent(
            q_id NUMBER NOT NULL PRIMARY KEY,
            vtm_desc TEXT NOT NULL PRIMARY KEY,
            question TEXT NOT NULL PRIMARY KEY
        ); ''')


def select_all(db, table):
    cursor = db.execute(f'SELECT * FROM {table}') 
    result = cursor.fetchall()
    return(result)

def create_answers_table(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS answers(
            user_id TEXT NOT NULL PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            answer_1 TEXT,
            answer_2 TEXT,
            answer_3 TEXT NOT NULL
        ); ''')
    
def insert_extracted_filename(db, table, data):
    db.execute(f'INSERT OR IGNORE INTO {table} VALUES (?, ?)', data) 
    db.commit()

def insert_3Column_DB_table(db, table, data):
    db.execute(f'INSERT OR IGNORE INTO {table} VALUES (?, ?, ?)', data) 
    db.commit()
    
def insert_vtm_archive(db, table, data):
    db.executemany(f'INSERT OR IGNORE INTO {table} VALUES (?, ?, ?, ?, ?)', data) 
    db.commit()

def select_all(db, table):
    cursor = db.execute(f'SELECT * FROM {table}') 
    result = cursor.fetchall()
    return(result)

def describe_table(db, table):
    from sqlite3 import OperationalError
    try:
        cursor = db.execute(
            f'SELECT sql FROM sqlite_master WHERE name = "{table}"'
            )
    except OperationalError:
        cursor = db.execute(
            f'SELECT sql FROM sqlite_master WHERE name = "{table}"'
            )
    result = cursor.fetchone()
    return(result)

def show_tables(db): 
    from sqlite3 import OperationalError
    try:
        cursor = db.execute('''
            SELECT name
            FROM sqlite_master
            WHERE type ='table' AND name NOT LIKE 'sqlite_%'
        ''')
    except OperationalError:
        cursor = db.execute('''
            SELECT name
            FROM sqlite_master
            WHERE type ='table' AND name NOT LIKE 'sqlite_%'
        ''')
    result = cursor.fetchall() 
    return(result)
