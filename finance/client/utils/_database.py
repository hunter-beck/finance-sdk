import sqlite3
from dataclasses import astuple, asdict

def createTable(db_path, table_definition):
    '''Creates table at the db_path with table_definition
    
    Args:
        db_path (Path): path defining the location of the sqlite3 database
        table_definition (str): defines the database structure (standard SQL formatting)
    '''
    
    db_connection = sqlite3.connect(db_path)
    cur = db_connection.cursor()
    cur.execute(table_definition)
    db_connection.commit()
    db_connection.close()
    
def updateMultipleRecords(db_path, table_name, records):
    '''INSERT OR REPLACE records in the specified table based on the parameters.
    
    Args:
        db_path (Path): db_path (Path): path defining the location of the sqlite3 database
        table_name (str): name of table in the database
        records (list): list of records with the same structure as 'values'
            NOTE: all records must be of the same dataclass
    '''
    
    values = tuple(asdict(records[0]).keys()) # uses first record as indicative of all records
    
    tuple_records = [astuple(record) for record in records]
    
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    
    val_placeholder = ','.join(['?']*len(values))
    
    update_query = f'''INSERT OR REPLACE INTO {table_name} {values} VALUES ({val_placeholder})'''
    print(update_query)
    cur.executemany(update_query, tuple_records)
    
    con.commit()
    cur.close()
    con.close()