from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import glob
import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def get_data(station:str):
    resp = urlopen('https://anson.ucdavis.edu/~clarkf/pems_parquet.zip')
    zipfile = ZipFile(BytesIO(resp.read()))
    for file in zipfile.namelist():
        if file.startswith(f'pems_sorted/{station}/part'):
            filename = file.split('-')[2]
            stat = station.replace("=","")
            filepath = f'{filename}.csv'
            outdir = f'../data/{stat}/'
            if not os.path.exists(outdir):
                os.mkdir(outdir)
            fullname = os.path.join(outdir, filepath)
            pd.read_parquet(file).to_csv(fullname)
    stat = station.replace("=","")
    path = f'../data/{stat}'
    all_files = glob.glob(path + "/*.csv")
    df_from_each_file = (pd.read_csv(f, sep=',') for f in all_files)
    df_merged = pd.concat(df_from_each_file, ignore_index=True)
    df_merged.rename(columns={'Unnamed: 0':'ID'},inplace = True)
    df_merged.to_csv(f'../data/{stat}_merged.csv',index = False)
    
def table_data(station:str):
    stat = station.replace("=","")
    df_merged = pd.read_csv(f'../data/{stat}_merged.csv')
    #create different csv files for the different files to be added to postgresql
    flow = df_merged[['flow1','flow2','flow3']]
    occupancy = df_merged[['occupancy1','occupancy2','occupancy3']]
    speed = df_merged[['speed1','speed2','speed3']]
    flow.to_csv(f'../data/{stat}_flow.csv',index = False)
    occupancy.to_csv(f'../data/{stat}_occupancy.csv',index = False)
    speed.to_csv(f'../data/{stat}_speed.csv',index = False)
    