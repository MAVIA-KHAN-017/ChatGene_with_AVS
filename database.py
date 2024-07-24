# import pyodbc
# from config import DB_CONFIG
# import redis
# import time

# # def get_database_connection():
# #     # try:
# #         connection_string = (
# #             f"DRIVER={{{DB_CONFIG['driver']}}};"
# #             f"SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};"
# #             f"UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']};"
# #             f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
# #         )
# #         connection = pyodbc.connect(connection_string)
# #         cursor = connection.cursor()
# #         return {"cursor":cursor,"connection":connection}
# #         # try:
# #         #     yield cursor
# #         # finally:
# #         #     cursor.close()
# #     # finally:
# #     #     connection.close()

# def get_database_connection():
#     connection_string = (
#             f"DRIVER={{{DB_CONFIG['driver']}}};"
#             f"SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};"
#             f"UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']};"
#             # f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
#     )
#     connection = pyodbc.connect(connection_string)
#     cursor = connection.cursor()
#     return {"cursor": cursor, "connection": connection}

# def execute_query_with_retry(query,params=None):
#     connection = None
#     cursor = None
    
#     while True:
#         if connection is None:
#             try:
#                 conn_dict = get_database_connection()
#                 connection = conn_dict["connection"]
#                 cursor = conn_dict["cursor"]
#             except pyodbc.Error as e:
#                 print("Error connecting to database:", e)
#                 time.sleep(1)
#                 continue

#         try:
#             if params:
#                 cursor.execute(query, params)
#             else:
#                 cursor.execute(query)
#             results = cursor.fetchall()
#             return results
#         except pyodbc.Error as pe:
#             print("Error executing query:", pe)
#             if pe.args[0] == "08S01": 
#                 try:
#                     connection.close()
#                 except:
#                     pass
#                 connection = None
#                 time.sleep(1)
#                 continue
#             else:
#                 raise

# redis_client = redis.Redis(host='localhost', port=6379, db=0)


import pyodbc
import redis
import time
from dotenv import load_dotenv
import os

load_dotenv()


DB_CONFIG = {
    'server': os.getenv('SERVER'),
    'database': os.getenv('DATABASE'),
    'username': os.getenv('USER_NAME'),
    'password': os.getenv('PASSWORD'),
    'driver': os.getenv('DRIVER'),
    'trusted_connection': os.getenv('TRUSTED_CONNECTION', 'no')
}


def get_database_connection():
    connection_string = (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']};"
            # f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
    )
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    return {"cursor": cursor, "connection": connection}

def execute_query_with_retry(query,params=None):
    connection = None
    cursor = None
    
    while True:
        if connection is None:
            try:
                conn_dict = get_database_connection()
                connection = conn_dict["connection"]
                cursor = conn_dict["cursor"]
            except pyodbc.Error as e:
                print("Error connecting to database:", e)
                time.sleep(1)
                continue

        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            return results
        except pyodbc.Error as pe:
            print("Error executing query:", pe)
            if pe.args[0] == "08S01": 
                try:
                    connection.close()
                except:
                    pass
                connection = None
                time.sleep(1)
                continue
            else:
                raise

redis_client = redis.Redis(host='localhost', port=6379, db=0)