import sqlite3
from sqlite3 import Error

from cx_logging import *

def create_test_table(conn):
    try:
        c = conn.cursor()
        c.execute('''
                  CREATE TABLE IF NOT EXISTS test_data(
                  ID INTEGER PRIMARY KEY AUTOINCREMENT,
                  NAME TEXT NOT NULL,
                  DATA TEXT NOT NULL)
                  ''')
        conn.commit()
        logger.info("done")
        return True
    except Error as e:
        logger.debug(e)
        return False


def insert_test_data_record(conn, NAME, DATA):
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO test_data (NAME, DATA)
                  VALUES (?, ?)''', (NAME, DATA))
        conn.commit()
        logger.info('db record : ' + NAME)
        return True
    except Error as e:
        logger.debug(e)
        return False

def create_FMT_table(conn):
    try:
        c = conn.cursor()
        c.execute('''
                  CREATE TABLE IF NOT EXISTS FMT(
                  ID INTEGER PRIMARY KEY AUTOINCREMENT,
                  TYPE INT NOT NULL,
                  LENGTH TEXT NOT NULL,
                  NAME TEXT UNIQUE NOT NULL,
                  FORMAT TEXT NOT NULL,
                  COLUMNS TEXT NOT NULL)
                  ''')
        # TODO : Dynamic table creation
        # sql = "CREATE TABLE IF NOT EXISTS [℅s] (℅s text, ℅s text)" ℅ (NAME, "first", " Second ")
        # c.execute(sql)
        conn.commit()
        logger.info("done")
        return True
    except Error as e:
        logger.debug(e)
        return False
    
def insert_FMT_data_record(conn, Type, Length, Name, Format, Columns):
    c = conn.cursor()
    try:
        c.execute('''INSERT OR IGNORE INTO FMT (TYPE, LENGTH, NAME, FORMAT, COLUMNS)
                  VALUES (?, ?, ?, ?, ?)''', (int(Type), Length, Name, Format, Columns))
        logger.info('db record : ' + Name)
        return True
    except Error as e:
        logger.debug(e)
        conn.commit()
        return False
    
def create_PARM_table(conn):
    try:
        c = conn.cursor()
        c.execute('''
                  CREATE TABLE IF NOT EXISTS PARM(
                  ID INTEGER PRIMARY KEY AUTOINCREMENT,
                  TIMEUS INT NOT NULL,
                  NAME TEXT UNIQUE NOT NULL,
                  VALUE TEXT NOT NULL,
                  DEFAULT_VAL TEXT NOT NULL)
                  ''')
        conn.commit()
        logger.info("done")
        return True
    except Error as e:
        logger.debug(e)
        return False
    
def insert_PARM_data_record(conn, Time, Name, Value, Default):
    c = conn.cursor()
    try:
        c.execute('''INSERT OR IGNORE INTO PARM (TIMEUS, NAME, VALUE, DEFAULT_VAL)
                  VALUES (?, ?, ?, ?)''', (int(Time), Name, Value, Default))
        logger.info('db record : ' + Name)
        return True
    except Error as e:
        logger.debug(e)
        conn.commit()
        return False

def create_DATA_table(conn):
    try:
        c = conn.cursor()
        c.execute('''
                  CREATE TABLE IF NOT EXISTS DATA(
                  ID INTEGER PRIMARY KEY AUTOINCREMENT,
                  ATTR TEXT NOT NULL,
                  TIMEUS INT NOT NULL,
                  VALUE1 TEXT,
                  VALUE2 TEXT,
                  VALUE3 TEXT,
                  VALUE4 TEXT,
                  VALUE5 TEXT,
                  VALUE6 TEXT,
                  VALUE7 TEXT,
                  VALUE8 TEXT,
                  VALUE9 TEXT,
                  VALUE10 TEXT,
                  VALUE11 TEXT,
                  VALUE12 TEXT,
                  VALUE13 TEXT,
                  VALUE14 TEXT,
                  VALUE15 TEXT,
                  VALUE16 TEXT)
                  ''')
        conn.commit()
        logger.info("done")
        return True
    except Error as e:
        print(e)
        logger.debug()
        return False

def insert_DATA_record(conn, Attr, Time, Value1=None, Value2=None, Value3=None, Value4=None, Value5=None, Value6=None, Value7=None, Value8=None, Value9=None, Value10=None, Value11=None, Value12=None, Value13=None, Value14=None, Value15=None, Value16=None):
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO DATA (ATTR, TIMEUS, VALUE1, VALUE2, VALUE3, VALUE4, VALUE5, VALUE6, VALUE7, VALUE8, VALUE9, VALUE10, VALUE11, VALUE12, VALUE13, VALUE14, VALUE15, VALUE16)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (Attr, int(Time), Value1, Value2, Value3, Value4, Value5, Value6, Value7, Value8, Value9, Value10, Value11, Value12, Value13, Value14, Value15, Value16))
        logger.info('db record : ' + Attr)
        return True
    except Error as e:
        print(e)
        logger.debug()
        conn.commit()
        return False
    
def create_tables(conn):
    ret = create_test_table(conn)
    ret = ret and create_FMT_table(conn)
    ret = ret and create_PARM_table(conn)
    ret = ret and create_DATA_table(conn)

    return ret