from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import glob
import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_db(db_host, db_user, db_password, db_name):
    """create a new database
    1. Create connection to Postgresql
    """
    conn = psycopg2.connect(host = db_host,user = db_user,password = db_password, database = 'postgres')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("CREATE DATABASE %s  ;" % db_name)
    cur.close()

def create_tables(db_host, db_user, db_password, db_name):    
    '''2. Create table in postgresql
    flow4 ,occupancy4,speed4...flow8,occupancy8,speed8 are null so we dont include them in the tables'''
    
    commands = (
        """
    CREATE TABLE flow (
    ID INTEGER NOT NULL,
    flow1 INTEGER NOT NULL,
    flow2 INTEGER NOT NULL,
    flow3 INTEGER NOT NULL)
    """,
    """CREATE TABLE occupancy (
    ID INTEGER NOT NULL,
    occupancy1 NUMERIC NOT NULL,
    occupancy2 NUMERIC NOT NULL,
    occupancy3 NUMERIC NOT NULL)
    """,
    """CREATE TABLE speed (
    ID INTEGER NOT NULL,
    speed1 NUMERIC NOT NULL,
    speed2 NUMERIC NOT NULL,
    speed3 NUMERIC NOT NULL)
    """)
    for command in commands:
        print("Creating tables...")
        try:
            conn = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_password)
            cur = conn.cursor()
            cur.execute(command)
            # commit this change
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.close()
            
def load_tables(db_host:str, db_user:str, db_password:str,db_name:str,station:str,table:str,cols:list):
    stat = station.replace("=","")
    conn = psycopg2.connect(host=db_host,user=db_user, password = db_password, database = db_name)
    cur = conn.cursor()
    with open(f'../data/{stat}_{table}.csv', 'r') as f:
        # Notice that we don't need the `csv` module.
        next(f) # Skip the header row.
        cur.copy_from(f, f'{table}', sep=',',columns=cols)
    conn.commit()