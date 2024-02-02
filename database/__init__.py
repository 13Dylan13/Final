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