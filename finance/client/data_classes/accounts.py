from dataclasses import dataclass, field
from finance.client.data_classes._base import *
import uuid

@dataclass
class Account():
    '''Individual bank account, investment fund, etc.'''
         
    name: str
    country_code: str
    label_id: str = None
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    
class AccountList(GenericList):
    '''List of Accounts'''
    
accounts_table_definition = '''
    CREATE TABLE accounts (
        id text PRIMARY KEY,
        name text NOT NULL,
        country_code text NOT NULL,
        label_id text,
        FOREIGN KEY (label_id) REFERENCES labels (id)
    )
'''