import pandas as pd
import collections
from dataclasses import dataclass, field
from finance.client.utils._database import retrieveData, updateMultipleRecords, createMultipleRecords, deleteMultipleRecords

class GenericAPI():
    '''Generic interface to SQLite database from resource objects
    '''
    def __init__(self, db_path):
        self._db_path = db_path

    def retrieve(self, ids):
        '''Retrieves specific resources based on id.
        
        Args:
            ids (list of str): ids to retrieve
        Returns:
            (list): objects with specified ids
        '''
        
        return retrieveData(
            db_path=self._db_path,
            table_name=self._table_name, 
            resource_type=self._resource_type,
            list_type=self._list_type,
            ids=ids
        )   
    
    def create(self, objects):
        '''Create a new object in the database.
        
        Args:
            objects (list of Account, Record, Label): items to be written to the database
        '''
        createMultipleRecords(
            db_path=self._db_path, 
            table_name=self._table_name,
            records=objects)
        
    def upsert(self, objects):
        '''Upsert the objects passed. 
        
        Args:
            objects (list of Account, Record, Label): objects to upsert
        '''
        
        updateMultipleRecords(
            db_path=self._db_path, 
            table_name=self._table_name,
            records=objects)
        
    def delete(self, ids):
        '''Delete the data from the database based on the specified ids
        
        Args:
            ids (str | list of str): ids to delete
        '''
        deleteMultipleRecords(
            db_path=self._db_path,
            table_name=self._table_name,
            ids=ids
        )

class GenericList(collections.abc.MutableSequence):
    '''List of resources (e.g., AccountRecords). Used as a base for all list type objects.
    '''
    def __init__(self):
        self._inner_list = list()
        
    def __repr__(self):      
        return repr(self.to_pandas())

    def _repr_html_(self):
        return self.to_pandas().to_html()
    
    def __len__(self):
        return len(self._inner_list)

    def __delitem__(self, index):
        self._inner_list.__delitem__(index)

    def insert(self, index, value):
        self._inner_list.insert(index, value)

    def __setitem__(self, index, value):
        self._inner_list.__setitem__(index, value)

    def __getitem__(self, index):
        return self._inner_list.__getitem__(index)

    def append(self, value):
        self.insert(len(self) + 1, value)
        
    def to_pandas(self):
        
        df = pd.DataFrame([vars(x) for x in self._inner_list])
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def retrieve(self, ids):
        '''Retrieves based on the unique id of the object
        
        Args:
            ids (list of str): unique id of object
            
        Returns:
            GenericList: list of objects corresponding to ids
        '''
        filtered_list = list(filter(lambda item: item.id in ids, self._inner_list))
        res = type(self)()
        
        for item in filtered_list:
            res.append(item)
        
        return res