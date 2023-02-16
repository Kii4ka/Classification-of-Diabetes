# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 09:46:22 2021

@author: Ray
"""
import pandas as pd
from sqlalchemy import create_engine
from multipledispatch import dispatch
import csv
from io import StringIO


@dispatch(str, int, str, str, str)
def engine(host='localhost', port='5432', dbname='postgres',
            user=None, password=None):
    """Connect to a database using sqlalchemy and psycopg2. 
    
    Args:
        host     (str) : Host address, default = localhost
        port     (int) : Port number, default = 5432
        dbname   (str) : Database name, default = postgres
        user     (str) : User name
        password (str) : Password
    
    Returns:
        The database engine
        
    Raises:
        Exception: Failed to connect to the database
    """
    try:
        e = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'
                          .format(user, password, host, port, dbname))
        return e
    except Exception as ex:
        raise ConnectionError(ex)

@dispatch(dict)
def engine(engineDetails):
    """Connect to a database using sqlalchemy and psycopg2. 
    
    Args:
        engineDetails (dict) : engine details dictionary with value:
            host       (str) : Host address
            port       (int) : Port number
            dbname     (str) : Database name
            user       (str) : User name
            password   (str) : Password
    
    Returns:
        The database engine
        
    Raises:
        Exception: Failed to connect to the database
    """
    try:
        e = engine(engineDetails['host'], engineDetails['port'], engineDetails['dbname'], 
                   engineDetails['user'], engineDetails['password'])
        return e
    except Exception as ex:
        raise ConnectionError(ex)

@dispatch(str, object)
def execute(query, engine):
    """Execute the query without a return (e.g., DDL or DCL statements).
    
    Args:
        query   (str)    : SQL query to execute
        engine  (engine) : Database engine object
    """
    try:
        conn = engine.connect().connection
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as ex:
        raise ConnectionError(ex)
    finally:
        if cursor != None: cursor.close()


@dispatch(str, dict)
def execute(query, engineDetails):
    """Execute the query without a return (e.g., DDL or DCL statements). This 
    opens and closes the connection.
    
    Args:
        query (str)        : SQL query to execute
        engineDetails (dict) : engine details dictionary with value:
            host       (str) : Host address
            port       (int) : Port number
            dbname     (str) : Database name
            user       (str) : User name
            password   (str) : Password
    """
    try:
        conn = engine(engineDetails).connect().connection
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as ex:
        raise ConnectionError(ex)
    finally:
        if cursor != None: cursor.close()
        

@dispatch(str, object)
def executeQuery(query, engine):
    """Execute the query and return a Pandas DataFrame (e.g., DML).
    
    Args:
        query  (str)    : SQL query to execute
        engine (engine) : Database engine object
        
    Returns:
        Query results in a Pandas DataFrame
    """
    try:
        return pd.read_sql_query(query, engine)
    except Exception as ex:
        raise ConnectionError(ex)
    
@dispatch(str, dict)
def executeQuery(query, engineDetails):
    """Execute the query and return a Pandas DataFrame (e.g., DML). This opens
    and closes the connection.
    
    Args:
        query (str)          : SQL query to execute
        engineDetails (dict) : engine details dictionary with value:
            host       (str) : Host address
            port       (int) : Port number
            dbname     (str) : Database name
            user       (str) : User name
            password   (str) : Password
        
    Returns:
        Query results in a Pandas DataFrame
    """
    try:
        e = engine(engineDetails)
        return executeQuery(query, e)
    except Exception as ex:
        raise ConnectionError(ex)
    finally:
        if e != None: e.dispose()
    

def psql_insert_copy(table, conn, keys, data_iter):
    """pandas.to_sql "method" for fast copy for databases that support "COPY".
    Example use: df.to_sql('table_name', sqlalchemy_engine, method=psql_insert_copy)

    Args:
        table       : pandas.io.sql.SQLTable
        conn        : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys        : list of str Column names
        data_iter   : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)
    
    
    
    