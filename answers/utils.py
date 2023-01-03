"""
Corteva Coding Exercise

CREATED BY: Jesse Fimbres
LAST MODIFIED: 12/29/2022
"""

import psycopg2
import psycopg2.extras

from pg_config import db_creds

def db_connect():
    """
    Connects to Postgres db through psycopg2
    Returns: psycopg2 connection object
    """
    pg_conn = psycopg2.connect(user=db_creds['USERNAME'],password=db_creds['PASSWORD'],\
        host=db_creds['HOST'],port=db_creds['PORT'],dbname=db_creds['DATABASE'])
    return pg_conn

def generate_where_clause(where_lst):
    """
    Formats where clause for SQL query
    Returns: String
    """
    where_lst = [x for x in where_lst if x[1] is not None]

    if len(where_lst) == 0:
        return str()
    str_temp = f" WHERE {where_lst[0][0]} = '{where_lst[0][1]}'"
    if len(where_lst) == 1:
        return str_temp
    return str_temp + f" AND {where_lst[1][0]} = '{where_lst[1][1]}'"

def get_data(schema,table,where_clause,pagination_clause,count=False):
    """
    Gets where and pagination filtered data from Postgres db
    Returns: String
    """
    selection = '*'
    if count:
        selection = 'COUNT(*)'
    session = db_connect()
    cur = session.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    query = f'''SELECT {selection} FROM {schema}.{table}{where_clause}{pagination_clause};'''
    cur.execute(query)
    res = cur.fetchall()

    cur.close()
    session.close()
    return res
