import pandas as pd
import collections
from dataclasses import dataclass, field, astuple, asdict
from finance.client.utils._database import retrieveData, writeData

class GenericAPI():
    '''Generic interface to SQLite database from resource objects
    '''
    def __init__(self, client):
        self._client = client

    def retrieve(self, ids):
        '''Retrieves specific resources based on id.
        
        Args:
            ids (list of str): ids to retrieve
        Returns:
            (list): objects with specified ids
        '''
        
        if len(ids) > 1:    
            query_filter = "WHERE id IN" + "(" + ",".join([f"'{id}'" for id in ids]) + ")"
        else:
            query_filter = f"WHERE id = '{ids[0]}'"
            
        retrieve_query = f'SELECT * FROM {self._table_name} {query_filter}'
                
        return retrieveData(
            client=self._client,
            query=retrieve_query,
            resource_type=self._resource_type,
            list_type=self._list_type,
        )   
    
    def create(self, objects):
        '''Create new objects in the database
        
        Args:
            objects (list of Account, Record, Label): items to be written to the database
        '''
        values = f"({','.join(asdict(objects[0]).keys())})"
        val_placeholder = ','.join(['?']*len(asdict(objects[0]).keys()))
        query = f'''INSERT INTO {self._table_name} {values} VALUES ({val_placeholder})'''
        tuple_values = [astuple(obj) for obj in objects]
        
        writeData(
            client=self._client,
            query=query,
            values=tuple_values,
            mode='many'
        )
        
        return objects
        
    def upsert(self, objects):
        '''Upsert the objects passed. 
        
        Args:
            objects (list of Account, Record, Label): objects to upsert
        '''
        test_object = asdict(objects[0])
        row_values = f"({','.join(test_object.keys())})"
        val_placeholder = ','.join(['?']*len(test_object.keys()))
        source_row_values = f"(Source.{', Source.'.join(test_object.keys())})"
        update_values = ','.join([f"{key}=Source.{key}" for key in test_object.keys()])

        query = f'''
            MERGE INTO {self._table_name} as Target
            USING (SELECT * FROM 
                (VALUES ({val_placeholder})) 
                AS s {row_values}
                ) AS Source
            ON Target.id=Source.id
            WHEN NOT MATCHED THEN
            INSERT {row_values} VALUES {source_row_values}
            WHEN MATCHED THEN
            UPDATE SET {update_values};
        '''
        
        tuple_values = [astuple(obj) for obj in objects]
        
        writeData(
            client=self._client, 
            query=query,
            values=tuple_values,
            mode='many'    
        )
        
        return objects
        
    def delete(self, ids):
        '''Delete the data from the database based on the specified ids
        
        Args:
            ids (str | list of str): ids to delete
        '''
        
        query = f'''DELETE FROM {self._table_name} WHERE id=?'''
        tuple_values = [tuple([i]) for i in ids]
        
        writeData(
            client=self._client,
            query=query,
            values=tuple_values,
            mode='many'
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