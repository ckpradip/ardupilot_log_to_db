from dbtest import *
from pymavlink import mavutil

def parser(conn, msg):

    # Adding each log entry into the database

    match msg.msg_type():
        case 'FMT':
            
            insert_test_data_record(conn, 'FMT', value)

    for key, value in msg.__dict__.items():
        print (msg)
        print(key)
        print(value)

        insert_test_data_record(conn, key, value)
