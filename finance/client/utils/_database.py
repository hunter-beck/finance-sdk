import sqlite3
from dataclasses import astuple, asdict
from sqlite3 import OperationalError
import pyodbc
import time

def createTable(db_path, table_definition):
    '''Creates table at the db_path with table_definition
    
    Args:
        db_path (Path): path defining the location of the sqlite3 database
        table_definition (str): defines the database structure (standard SQL formatting)
    '''
    
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    try:
        cur.execute(table_definition)
    except:
        cur.close()
        con.close()
        raise
    
    con.commit()
    con.close()
    

def createMultipleRecords(client, table_name, records):
    '''INSERT records in the specified table based on the parameters.
    
    Args:
        client (Client): Finance client with connection details
        table_name (str): name of table in the database
        records (iterable): list of records with the same structure as 'values'
            NOTE: all records must be of the same dataclass
    '''
    
    values = f"({','.join(asdict(records[0]).keys())})"
    
    tuple_records = [astuple(record) for record in records]
    
    con = dbConnection(client)
    cur = con.cursor()
    
    val_placeholder = ','.join(['?']*len(asdict(records[0]).keys()))
    
    update_query = f'''INSERT INTO {table_name} {values} VALUES ({val_placeholder})'''
    try:
        cur.executemany(update_query, tuple_records)
    except:
        cur.close()
        con.close()
        raise
    
    con.commit()
    cur.close()
    con.close()
    
def updateMultipleRecords(db_path, table_name, records):
    '''INSERT OR REPLACE records in the specified table based on the parameters.
    
    Args:
        db_path (Path): db_path (Path): path defining the location of the sqlite3 database
        table_name (str): name of table in the database
        records (list): list of records with the same structure as 'values'
    '''
    
    if type(records) != list:
        records = [records]
    
    values = tuple(asdict(records[0]).keys()) # uses first record as indicative of all records
    
    tuple_records = [astuple(record) for record in records]
    
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    
    val_placeholder = ','.join(['?']*len(values))
    
    update_query = f'''REPLACE INTO {table_name} {values} VALUES ({val_placeholder})'''
    
    try:
        cur.executemany(update_query, tuple_records)
    except:
        cur.close()
        con.close()
        raise
    
    con.commit()
    cur.close()
    con.close()
    

def retrieveData(db_path, table_name, resource_type, list_type, ids):
    '''Check wether the table exists (create the table if not found 
    and add them to the list object passed to the function.
    
    Args:
        db_path (Path): path to the SQLite3 database
        table_name (str): name of the sql table
        resource_type (type): resource type to construct list of
        list_type (type): type of list object which will be constructed
        ids (list of str): specific ids to retrieve
    Returns:
        (List Object): list object with appended data based on object_type
    '''
    
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    
    if len(ids) > 1:    
        query_filter = ' OR '.join([f'id="{id}"' for id in ids])
    else:
        query_filter = f'id = "{ids[0]}"'
        
    try:
        cur.execute(f'SELECT * FROM {table_name} WHERE {query_filter}')
    except:
        cur.close()
        con.close()
        raise

    rows = cur.fetchall()
    cur.close()
    con.close()
    
    return _contructList(rows, resource_type, list_type)


def filterData(db_path, query, resource_type, list_type):
    '''Check wether the table exists (create the table if not found 
    and add them to the list object passed to the function.
    
    Args:
        db_path (Path): path to the SQLite3 database
        query (str): specifying query parameters for data retrieval
        table_definition (str): defines the format of the sql table
        resource_type (type): resource type to construct list of
        list_type (type): type of list object which will be constructed
    Returns:
        (List Object): list object with appended data based on object_type
    '''
    
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    try:
        cur.execute(query)
    except:
        cur.close()
        con.close()
        raise
        
    rows = cur.fetchall()
    cur.close()
    con.close()
    
    return _contructList(rows, resource_type, list_type)


def _contructList(rows, resource_type, list_type):
    '''Consumes rows from database read and constructs a list based on resource_type
    
    Args:
        rows (list of sqlite3.Row): rows returned from database
        resource_type (type): resource type to construct list of
        list_type (type): type of list object which will be constructed
    '''
    list_object = list_type()
    
    for row in rows:
        values = list(row)
        keys = row.keys()
        row_dict = dict(zip(keys,values))
        list_object.append(resource_type(**row_dict))
        
    return list_object


def deleteMultipleRecords(db_path, table_name, ids):
    '''DELETE records in the specified table based on the parameters.
    
    Args:
        db_path (Path): db_path (Path): path defining the location of the sqlite3 database
        table_name (str): name of table in the database
        ids (str OR list of str): ids of the records to delete
    '''
    
    if type(ids) != list:
        ids = [ids]
    
    con = sqlite3.connect(db_path)
    cur = con.cursor()
        
    delete_query = f'''DELETE FROM {table_name} WHERE id=?'''
    
    try:
        for id in ids:
            cur.execute(delete_query, (id,))
            
    except:
        cur.close()
        con.close()
        raise
    
    con.commit()
    cur.close()
    con.close()
    
    
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