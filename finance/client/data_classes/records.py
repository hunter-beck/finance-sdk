from dataclasses import dataclass, field
from finance.client.data_classes._base import *
import pandas as pd
from datetime import datetime
import uuid


@dataclass
class AccountRecord():
    '''Individual record for a balance at a point in time for an account'''
    
    datetime: datetime
    account_id: str
    balance: float
    currency: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    
class AccountRecordList(GenericList):
    '''List of AccountRecords'''
    
    def getMostRecentRecord(self, account_ids, end_time=datetime.now()):
        '''Retrieves the value (based on the newest AccountRecord) for the specified account_id
        
        Args:
            account_ids (list of str): unique ids for the accounts
            end_time (datetime): defaults to now, but can specify time in past to get 
                balance on that date (inclusive)
        
        Returns:
            (AccountRecordList): list of most recent account records for account_ids specified
        '''
                
        records = self.to_pandas()
        filtered_records = records[(records['account_id'].isin(account_ids)) & (records['datetime'] <= end_time)]        
        idx = filtered_records.groupby(['account_id'])['datetime'].transform(max) == filtered_records['datetime']
        grouped_records = filtered_records[idx]        
        
        return self.retrieve(ids=list(grouped_records['id']))
    
    def getAccountRecords(self, account_ids):
        '''Retrieves the account records for the account_ids specified
        
        Args: 
            account_ids (str or list of str): unique id(s) for the accounts
            
        Returns:
            (AccountRecordList): list of records corresponding to account ids specified
        '''
        
        if type(account_ids) == str:
            acccount_ids = list(account_ids)
        
        filtered_records = filter(lambda account_record: account_record.account_id in account_ids, self)
        
        res = AccountRecordList()
        
        res._inner_list = list(filtered_records)
        
        return res