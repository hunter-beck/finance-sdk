from dataclasses import dataclass, field
from finance.client.data_classes._base import *
import pandas as pd
from datetime import datetime
import uuid
from finance.client.utils._database import filterData

@dataclass
class Record():
    '''Individual record for a balance at a point in time for an account'''
    
    date: datetime
    account_id: str
    balance: float
    currency: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    
class RecordList(GenericList):
    '''List of Records'''
    
    def getMostRecentRecord(self, account_ids, end_time=datetime.now()):
        '''Retrieves the value (based on the newest Record) for the specified account_id
        
        Args:
            account_ids (list of str): unique ids for the accounts
            end_time (datetime): defaults to now, but can specify time in past to get 
                balance on that date (inclusive)
        
        Returns:
            (RecordList): list of most recent account records for account_ids specified
        '''
                
        records = self.to_pandas()
        filtered_records = records[(records['account_id'].isin(account_ids)) & (records['datetime'] <= end_time)]        
        idx = filtered_records.groupby(['account_id'])['datetime'].transform(max) == filtered_records['datetime']
        grouped_records = filtered_records[idx]        
        
        return self.retrieve(ids=list(grouped_records['id']))
    
    def getRecords(self, account_ids):
        '''Retrieves the account records for the account_ids specified
        
        Args: 
            account_ids (str or list of str): unique id(s) for the accounts
            
        Returns:
            (RecordList): list of records corresponding to account ids specified
        '''
        
        if type(account_ids) == str:
            acccount_ids = list(account_ids)
        
        filtered_records = filter(lambda record: record.account_id in account_ids, self)
        
        res = RecordList()
        
        res._inner_list = list(filtered_records)
        
        return res
    
    
class RecordsAPI(GenericAPI):
    
    _table_name = 'records'
    _table_definition = '''
        CREATE TABLE records (
            id text PRIMARY KEY,
            date text NOT NULL,
            account_id text NOT NULL,
            balance real NOT NULL,
            currency text NOT NULL,
            FOREIGN KEY (account_id) REFERENCES accounts (id)
        )
    '''
    
    _resource_type = Record
    _list_type = RecordList
    
    def list(self, account_id=None, currency=None):
        '''Retrieves a list of records based on the criteria. 

        Args:
            account_id (str): unique id of the account on the record
            currency (str): currency code for the record
        Returns:
            (list): objects meeting filter criteria
        '''
        
        query_list = []
            
        if account_id:
            account_id_query = f'account_id = "{account_id}"'
            query_list.append(account_id_query)

        if currency:
            currency_query = f'currency = "{currency}"'
            query_list.append(currency_query)
            
        if len(query_list) > 0:
            query = 'WHERE ' + ' AND '.join(query_list)
        else:
            query = ''
            
        return filterData(
            db_path=self._db_path, 
            table_name=self._table_name,
            resource_type=self._resource_type,
            list_type=self._list_type,
            query_filter=query)
