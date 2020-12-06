from dataclasses import dataclass, field
from finance.client.data_classes._base import *
import uuid
from finance.client.utils._database import filterData

@dataclass
class Account():
    '''Individual bank account, investment fund, etc.'''
         
    name: str
    country_code: str
    label_id: str = None
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    
    
class AccountList(GenericList):
    '''List of Accounts'''
    
 
class AccountsAPI(GenericAPI):
    
    _table_name = 'accounts'
    
    _table_definition = '''
        CREATE TABLE accounts (
            [id] varchar(10) PRIMARY KEY,
            [name] text NOT NULL,
            [country_code] text NOT NULL,
            [label_id] varchar(10),
            FOREIGN KEY ([label_id]) REFERENCES labels ([id])
        )
    '''
    
    _resource_type = Account
    _list_type = AccountList
    
    def list(self, name=None, country_code=None, label_id=None):
        '''Retrieves a list of accounts based on the criteria. 

        Args:
            name (str): name of account
            country_code (str): country code for region (e.g., USA)
            label_id (str): unique id of the label on the account
        Returns:
            (list): objects meeting filter criteria
        '''
        
        query_list = []
            
        if name:
            name_query = f'name = "{name}"'
            query_list.append(name_query)

        if country_code:
            country_code_query = f'country_code = "{country_code}"'
            query_list.append(country_code_query)
            
        if label_id:
            label_query = f'label_id = "{label_id}"'
            query_list.append(label_query)
            
        if len(query_list) > 0:
            filter = 'WHERE ' + ' AND '.join(query_list)
        else:
            filter = ''
            
        query = f'SELECT * FROM {self._table_name} {filter}'
            
        return filterData(
            db_path=self._db_path, 
            query=query,
            resource_type=self._resource_type,
            list_type=self._list_type,
        )
            
            