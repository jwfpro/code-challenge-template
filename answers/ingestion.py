#!/usr/bin/env python

"""
Module for ingesting csv data to Postgres.

This module contains the IngestionBackend class which handles preprocessing of 
csv files, creates any required schemas or tables to house the data, then uploads
to the Postgres DB and performs some stats analysis.
"""

__author__ = "Jesse Fimbres"
__date__ = "01/02/2023"


import io
import time
import os
import logging
from pathlib import Path

import numpy as np
import psycopg2
import psycopg2.extras
import pandas as pd
from psycopg2.extensions import register_adapter, AsIs

import constants as const
from utils import db_connect

class IngestionBackend():
    """
    Class writes csv files to Postgres and performs stats calcuations

    Attributes:
        schema: schema name for current dataset
        table: table name for current dataset
    """

    def __init__(self,schema,table):
        self.schema = schema
        self.table = table

    def ingest_data(self,query):
        """
        Reads data from local dir, ingests into Postgres db
        """
        logging.info('Ingesting data: ' + self.table)
        data_is_wx = self.table == 'wx_data'
        keys,data_dir,data_cols = (['FILE','DATE'],const.WX_DATA,const.WX_COLS) if data_is_wx \
        else (['YEAR'],const.YLD_DATA,const.YLD_COLS)

        file_list = [file for file in os.listdir(data_dir) if file.endswith('.txt')]
        df = pd.concat([self.ingest_data_helper(file,data_dir,data_cols) for file in file_list])
        df = df.drop_duplicates(keys)

        if data_is_wx:
            df.rename(columns={'FILE':'STATION_ID'}, inplace=True)
            df['DATE'] = pd.to_datetime(df['DATE'].astype(str), format='%Y%m%d')
        else:
            df.drop('FILE', axis=1, inplace=True)

        self.create_table(self.table,query)
        self.upload_to_db(df)

    def ingest_data_helper(self,file,data_dir,cols):
        """
        Reads csv as DataFrame, adds FILE col
        Returns: Pandas DataFrame
        """
        df = pd.read_csv(f'{data_dir}/{file}', sep='\t', names=cols)
        df['FILE'] = Path(file).stem
        return df

    def create_table(self,table,query):
        """
        Creates schema and table in Postgres if not exists
        """
        logging.info(f'Creating schema (if not exists): {self.schema}')
        session = db_connect()
        cur = session.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {self.schema}')

        logging.info(f'Creating table (if not exists): {self.schema}.{table}')

        cur.execute(query)
        session.commit()
        cur.close()
        session.close()

    def upload_to_db(self,df):
        """
        Writes df to Postgres db, gets rows added count
        """
        logging.info(f'Writing to table: {self.schema}.{self.table}')
        psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
        session = db_connect()
        cur = session.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

        cur.execute(f'SELECT COUNT(*) FROM {self.schema}.{self.table}')
        prev_count = cur.fetchone()['count']

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, date_format='%Y-%m-%d')
        buffer.seek(0)

        query = f'''
        BEGIN;
        CREATE TEMP TABLE tmp_table 
        (LIKE {self.schema}.{self.table} INCLUDING DEFAULTS)
        ON COMMIT DROP;
            
        COPY tmp_table({','.join(df.columns)}) FROM STDOUT CSV HEADER;
            
        INSERT INTO {self.schema}.{self.table}
        SELECT *
        FROM tmp_table
        ON CONFLICT DO NOTHING;
        COMMIT;
        '''
        cur.copy_expert(query,buffer)
        session.commit()

        cur.execute(f'SELECT COUNT(*) FROM {self.schema}.{self.table}')
        new_count = cur.fetchone()['count']
        logging.info(f'Rows added: {str(new_count-prev_count)}')
        cur.close()
        session.close()

    def generate_avg_table(self):
        """
        Creates average table, fills with filtered data
        """
        logging.info('Creating averages table (if not exists)')

        query = f'''
        CREATE TABLE IF NOT EXISTS {self.schema}.{const.AVG_TABLE}(
            STATION_ID VARCHAR(20),
            YEAR int4,
            AVG_MAX_TEMP int4,
            AVG_MIN_TEMP int4,
            TOTAL_PRECIPITATION int4,
            PRIMARY KEY(STATION_ID,YEAR)
        );
        '''
        self.create_table(const.AVG_TABLE,query)

        logging.info('Writing to averages table')
        session = db_connect()
        cur = session.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

        query = f'''
        INSERT INTO {self.schema}.{const.AVG_TABLE}
        (STATION_ID, YEAR, AVG_MAX_TEMP, AVG_MIN_TEMP, TOTAL_PRECIPITATION)
        SELECT
            STATION_ID,
            EXTRACT(YEAR FROM DATE) as YYYY,
            AVG(MAX_TEMP_1_10_DEG_C) FILTER (WHERE MAX_TEMP_1_10_DEG_C <> -9999) / 10,
            AVG(MIN_TEMP_1_10_DEG_C) FILTER (WHERE MIN_TEMP_1_10_DEG_C <> -9999) / 10,
            SUM(PRECIPITATION_1_10_mm) FILTER (WHERE PRECIPITATION_1_10_mm <> -9999) / 100
        FROM
            {self.schema}.{self.table}
        GROUP BY
            STATION_ID,
            EXTRACT(YEAR FROM DATE)
        ON CONFLICT DO NOTHING;
        '''
        cur.execute(query)
        session.commit()
        cur.close()
        session.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(asctime)s: %(filename)s: %(lineno)d : %(message)s',
        datefmt='%d-%b-%y %H:%M:%S')

    start_time = int(time.process_time() * 1000)
    logging.info(f'{"*" * 20} PROGRAM STARTED {"*" * 20}')

    query_wx = f'''
    CREATE TABLE IF NOT EXISTS {const.WX_SCHEMA}.{const.WX_TABLE}(
        STATION_ID VARCHAR(20),
        DATE date,
        MIN_TEMP_1_10_DEG_C int4,
        MAX_TEMP_1_10_DEG_C int4,
        PRECIPITATION_1_10_mm int4,
        PRIMARY KEY(STATION_ID,DATE)
    );
    '''
    query_yld = f'''
    CREATE TABLE IF NOT EXISTS {const.YLD_SCHEMA}.{const.YLD_TABLE}(
        YEAR int4 PRIMARY KEY,
        TOTAL_CORN_GRAIN_YIELD_MT int4
    );
    '''

    Ingestion_Obj_WX = IngestionBackend(const.WX_SCHEMA,const.WX_TABLE)
    Ingestion_Obj_WX.ingest_data(query_wx)

    Ingestion_Obj_YLD = IngestionBackend(const.YLD_SCHEMA,const.YLD_TABLE)
    Ingestion_Obj_YLD.ingest_data(query_yld)

    elapsed_time = int(time.process_time() * 1000) - start_time
    logging.info(f'Elapsed time (ms): {str(elapsed_time)}')

    Ingestion_Obj_WX.generate_avg_table()

    logging.info(f'{"*" * 20} PROGRAM COMPLETED {"*" * 20}')
