##### IMPORTS #####################################################################################
import databaseUtils as db
from io import StringIO
import os
import pandas as pd
import requests
import time


##### PRINTERS AND TIMERS #########################################################################
def printAndStartTimer(action, title):
    """
    Print 'action title...' and return time.time().
    
    Args:
        action (str) : action being taken
        title  (str) : title
    
    Returns:
        The start time as time.time()
    """
    print(action, title, '...')
    return time.time()

def printCompleteAndTime(action, startTime):
    """
    Print 'action complete: time.time()-startTime seconds'.
    
    Args:
        action            (str) : action being taken
        startTime (time.time()) : start time from time.time()
    """
    print(action, 'complete:', time.time()-startTime, 'seconds')


##### WRITE DATA TO DISK ##########################################################################
def write(data, file, mode, newline=os.linesep):
    """
    Writes the data to the file in the given mode.
    
    Args:
        data    (str) : data to write
        file    (str) : path and file name
        mode    (str) : open mode (a = append or w = write)
        newline (str) : new line character (default: os.linesep -> \r\n)
    """
    with open(os.path.join(os.getcwd(), file), mode, encoding='utf-8', newline=newline) as f:
        f.write(data)
        
def writeTextToFile(text, file, title=None, append=False, header=True, newline=os.linesep):
    """
    Writes the text to the file.
    
    Args:
        text       (str) : text to write
        file       (str) : path and file name
        title      (str) : title for printing (default: None)
        append (boolean) : append (default: False - write)
        header (boolean) : include header (default: True - include header)
        newline    (str) : new line character (default: os.linesep -> \r\n)
    """
    # set text based on mode
    text1 = 'Writing'
    text2 = 'Write'
    mode = 'w'
    if(append):
        text1 = 'Appending'
        text2 = 'Append'
        mode = 'a'
    if(header == False):
        text = text.replace(text.partition('\n')[0], '')
    
    # execute
    st = printAndStartTimer(text1, title)
    write(text, file, mode, newline)
    printCompleteAndTime(text2, st)

def writeDFToCSV(df, file, title=None, append=False, header=True, index=False):
    """
    Writes the data to the file.
    
    Args:
        df   (DataFrame) : pandas.DataFrame to write
        file       (str) : path and file name
        title      (str) : title for printing (default: None)
        append (boolean) : append (default: False - write)
        header (boolean) : include header (default: True - include header)
        index  (boolean) : include index (default: False - no index)
    """
    # set text based on mode
    text1 = 'Writing'
    text2 = 'Write'
    mode = 'w'
    if(append):
        text1 = 'Appending'
        text2 = 'Append'
        mode = 'a'
        
    # execute
    st = printAndStartTimer(text1, title)
    df.to_csv(file, mode=mode, index=index, header=header)
    printCompleteAndTime(text2, st)


##### DOWNLOAD FILES ##############################################################################
def downloadText(url, title=None):
    """
    Downloads and returns the text (requests.get(url).text) of the URL provided.
    
    Args:
        url   (str) : URL of text to download
        title (str) : title for printing (default: None)
    
    Returns:
        The text of the URL (requests.get(url).text).
    """
    st = printAndStartTimer('Downloading', title)
    ret = requests.get(url).text
    printCompleteAndTime('Download', st)
    return ret

def downloadCSV(url, title=None):
    """
    Downloads and returns the CSV at URL as a pandas.DataFrame.
    
    Args:
        url   (str) : URL of CSV data to download
        title (str) : title for printing (default: None)
        
    Returns:
        A pandas.DataFrame object of the CSV data from URL.
    """
    st = printAndStartTimer('Downloading', title)
    ret = pd.read_csv(url)
    printCompleteAndTime('Download', st)
    return ret

def downloadToFile(url, file, title=None, append=False, header=True, newline=os.linesep):
    """
    Downloads and writes the text (requests.get(url).text) of the URL provided to the file.
    
    Args:
        url        (str) : URL of CSV data to download
        file       (str) : path and file name
        title      (str) : title for printing (default: None)
        append (boolean) : append (default: False - write)
        newline    (str) : new line character (default: os.linesep -> \r\n)
    """
    # set text based on mode
    text1 = 'Downloading and writing to CSV'
    text2 = 'Download and write'
    mode = 'w'
    if(append):
        text1 = 'Downloading and appending to CSV'
        text2 = 'Download and append'
        mode = 'a'
    
    # execute
    st = printAndStartTimer(text1, title)
    resp = requests.get(url).text
    if(header == False):
        resp = resp.replace(resp.partition('\n')[0], '')
    write(resp, file, mode, newline)
    printCompleteAndTime(text2, st)
        

def downloadToFileStream(url, file, title=None):
    """
    Downloads and writes, in streaming fashion, the binary chunks (requests.get(url, stream=True)) 
    of the URL provided to the file.
    
    Args:
        url   (str) : URL of CSV data to download
        file  (str) : path and file name
        title (str) : title for printing (default: None)
    """
    st = printAndStartTimer('Downloading and writing, using a stream, to CSV', title)
    with requests.get(url, stream=True) as resp: # use with since it's streaming for safety
        with open(os.path.join(os.getcwd(), file), 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):  # 8*1024B
                f.write(chunk)
    printCompleteAndTime('Download and write with stream', st)



##### COPY FROM CSV ###############################################################################
def copyFromCSV(schema, table, file, engineDetails):
    """
    Copies (using COPY) the contents of the CSV file to the database as schema.table.
    
    Args:
        schema         (str) : schema
        table          (str) : table
        file           (str) : path and file name
        engineDetails (dict) : engine details dictionary with value:
            host       (str) : Host address
            port       (int) : Port number
            dbname     (str) : Database name
            user       (str) : User name
            password   (str) : Password
    """
    st = printAndStartTimer('Copying from CSV to', schema+'.'+table)
    db.execute("COPY {0}.{1} FROM '{2}' CSV HEADER ENCODING 'WIN1251'"
               .format(schema, table, os.path.join(os.getcwd(), file)), engineDetails)
    printCompleteAndTime('COPY', st)

def copyFromDF(schema, table, df, engineDetails):
    """
    Copies (using COPY) the contents of the DataFrame df to the database as schema.table.
    
    Args:
        schema         (str) : schema
        table          (str) : table
        df       (DataFrame) : path and file name
        engineDetails (dict) : engine details dictionary with value:
            host       (str) : Host address
            port       (int) : Port number
            dbname     (str) : Database name
            user       (str) : User name
            password   (str) : Password
    """
    st = printAndStartTimer('Copying from DataFrame to', schema+'.'+table)
    df.to_sql(table, db.engine(engineDetails), schema=schema, if_exists='fail', index=False, method=db.psql_insert_copy)
    printCompleteAndTime('COPY', st)

def copyFromSTDIN(schema, table, text, engineDetails):
    """
    Copies (using COPY) from the CSV as text using STDIN to the database as schema.table.
    
    Args:
        schema         (str) : schema
        table          (str) : table
        text           (str) : CSV as text
        engineDetails (dict) : engine details dictionary with value:
            host       (str) : Host address
            port       (int) : Port number
            dbname     (str) : Database name
            user       (str) : User name
            password   (str) : Password
    """
    schema_table = schema+'.'+table
    st = printAndStartTimer('Copying from STDIN CSV to', schema_table)
    conn = db.engine(engineDetails).connect().connection
    with conn.cursor() as cur:
        s_buf = StringIO(text)
        sql = 'COPY {} FROM STDIN WITH CSV HEADER'.format(schema_table)
        cur.copy_expert(sql=sql, file=s_buf)
        conn.commit()
    printCompleteAndTime('Copied from STDIN CSV', st)

def downloadAndCopyFromCSV(schema, table, url, engineDetails):
    """
    Copies (using COPY) the downloaded text (requests.get(url).text) from URL to the database as schema.table.
    
    Args:
        schema         (str) : schema
        table          (str) : table
        url            (str) : URL of CSV data to download
        engineDetails (dict) : engine details dictionary with value:
            host       (str) : Host address
            port       (int) : Port number
            dbname     (str) : Database name
            user       (str) : User name
            password   (str) : Password
    """
    schema_table = schema+'.'+table
    st = printAndStartTimer('Downloading and copying from CSV to', schema_table)
    conn = db.engine(engineDetails).connect().connection
    with conn.cursor() as cur:
        s_buf = StringIO(requests.get(url).text)
        sql = 'COPY {} FROM STDIN WITH CSV HEADER'.format(schema_table)
        cur.copy_expert(sql=sql, file=s_buf)
        conn.commit()
    printCompleteAndTime('Downloaded and copied from CSV', st)




