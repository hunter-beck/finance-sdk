import sqlite3

def createTable(self, table_definition):
    '''Creates table at the db_path with table_definition
    
    Args:
        table_definition (str): defines the database structure (standard SQL formatting)
    '''
    
    db_connection = sqlite3.connect(self._db_path)
    cur = db_connection.cursor()
    cur.execute(table_definition)
    db_connection.commit()
    db_connection.close()