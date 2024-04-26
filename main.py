import os

from hash import *
from database import *
from cxlogs import *
from logging import *

from pymavlink import mavutil
from datetime import datetime


SCRIPT_VERSION = "1.0.0"
DEBUG = True


print("script started")

log_folder_path = "P:\\Working\\60_Projects\\CXP_py_log_to_db\\test_logs\\00000040.BIN"



def main(log_folder_path):
    # initialise logging
    init_logger()

    print("main started")

    new_file = False

    conn = create_connection()
    create_table(conn)
    
    if log_folder_path.endswith('.BIN'):
        if insert_file_record(conn, os.path.basename(log_folder_path), os.path.dirname(log_folder_path), md5_hash_file(log_folder_path), SCRIPT_VERSION, datetime.now()):
            new_file = True
            print("File record inserted")
        else:
            print("File record already existing in database")
        
        if new_file or DEBUG:
            read_all_and_update_db(log_folder_path)

    close_connection(conn)
    print("main ended")


main (log_folder_path)  

print("script ended")