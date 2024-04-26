import os
import json
import sqlite3
from sqlite3 import Error

def create_connection():
    """ create a database connection to a SQLite database """
    # Open the JSON file
    with open('configs\\database.json') as f:
        data = json.load(f)
        
    # Get the database file path
    db_file = os.path.join(data['database']['path'], data['database']['name'])
    print(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)

    return conn


def close_connection(conn):
    conn.commit()
    conn.close()
    print("Connection closed")


def create_table(conn):
    try:
        c = conn.cursor()
        c.execute('''
                  CREATE TABLE IF NOT EXISTS file_records(
                  ID INTEGER PRIMARY KEY AUTOINCREMENT,
                  NAME TEXT NOT NULL,
                  PATH TEXT NOT NULL,
                  HASH TEXT NOT NULL UNIQUE,
                  SCRIPT_VERSION TEXT,
                  ANALYSIS_DATE DATETIME)
                  ''')
        conn.commit()
    except Error as e:
        print(e)


def insert_file_record(conn, NAME, PATH, HASH, SCRIPT_VERSION, ANALYSIS_DATE):
    c = conn.cursor()
    c.execute("SELECT * FROM file_records WHERE HASH = ?", (HASH,))
    data = c.fetchone()
    print(data)
    if data is None:
        # HASH is unique
        try:
            c.execute('''
                      INSERT INTO file_records (NAME, PATH, HASH, SCRIPT_VERSION, ANALYSIS_DATE)
                      VALUES (?, ?, ?, ?, ?)
                      ''', (NAME, PATH, HASH, SCRIPT_VERSION, ANALYSIS_DATE))
            conn.commit()
            return True
        except Error as e:
            print(e)
            return False
    else:
        # HASH is not unique
        print("File already exists in database")
        return False

