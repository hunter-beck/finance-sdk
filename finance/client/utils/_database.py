import sqlite3
from dataclasses import astuple, asdict
from sqlite3 import OperationalError
import pyodbc
import time

def writeData(client, query, values, mode='single'):
    '''execute multiple record creations in the specified table based on the parameters.
    
    Args:
        client (Client): Finance client with connection details
        query (str): specifying query parameters for data retrieval
        values (list of tuple): list of values for each record to write
            Note: can pass `None`
        mode (str): `single` or `many`
    '''
    
    con = dbConnection(client)
    cur = con.cursor()
    
    try:
        if mode=='single':
            cur.execute(query)
        elif mode=='many':
            cur.executemany(query, values)
        else:
            raise ValueError(f'mode:{mode}: not valid')
    except:
        cur.close()
        con.close()
        raise
    
    con.commit()
    cur.close()
    con.close()
    
    
def retrieveData(client, query, resource_type, list_type):
    '''Retrieves list of records based on query
    
    Args:
        client (Client): Finance client with connection details
        query (str): specifying query parameters for data retrieval
        resource_type (type): resource type to construct list of
        list_type (type): type of list object which will be constructed
    Returns:
        (List Object): list object with appended data based on object_type
    '''
    
    con = dbConnection(client)
    cur = con.cursor()

    try:
        cur.execute(query)
    except:
        cur.close()
        con.close()
        raise
        
    columns = [column[0] for column in cur.description]
    results = []
    for row in cur.fetchall():
        results.append(dict(zip(columns, row)))
    
    cur.close()
    con.close()
    
    return _contructList(results, resource_type, list_type)


def _contructList(rows, resource_type, list_type):
    '''Consumes rows from database read and constructs a list based on resource_type
    
    Args:
        rows (list of sqlite3.Row): rows returned from database
        resource_type (type): resource type to construct list of
        list_type (type): type of list object which will be constructed
    '''
    list_object = list_type()
    
    for row in rows:
        values = list(row.values())
        keys = row.keys()
        row_dict = dict(zip(keys,values))
        list_object.append(resource_type(**row_dict))
        
    return list_object
    
    
def dbConnection(client, timeout_retries=5):
    '''Establishes connection to either SQLite or Azure SQL DB
    
    Args:
        client (Client): Finance client with connection details
    '''
    
    if client._db_type == 'azure-sql':
    
        for i in range(timeout_retries):
            try: 
                return pyodbc.connect(
                    'DRIVER='+client._driver+\
                    ';SERVER='+client._server+\
                    ';PORT=1433'+\
                    ';DATABASE='+client._database+\
                    ';UID='+client._username+\
                    ';PWD='+ client._password
                )
            
            except TimeoutError: 
                time.sleep(1)
            
                if i == (timeout_retries-1):
                    raise
    
    elif client._db_type == 'sqlite':
        
        return sqlite3.connect(client._db_path)

    else:
        raise ValueError(f'{client._db_type}: does not match accepted db_types')