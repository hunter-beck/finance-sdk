from dataclasses import dataclass, field
from finances.client.data_classes._base import *

@dataclass
class Account():
    '''Individual bank account, investment fund, etc.'''
         
    name: str
    type: str
    country_code: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    
class AccountList(UserList):
    '''List of Accounts'''