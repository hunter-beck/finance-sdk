from finance.client.data_classes._base import *
from dataclasses import dataclass, field
import uuid
from finance.client.utils._database import retrieveData

@dataclass
class Label():
    '''Generic class for labeling of objects'''
    
    name: str
    description: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    _client_resource_class='labels'

    
class LabelList(GenericList):
    '''List of Type'''
    
    
class LabelsAPI(GenericAPI):
    
    _table_name = 'labels'
    _table_definition = '''
        CREATE TABLE labels (
            id varchar(10) PRIMARY KEY,
            name text NOT NULL,
            description text
        )
    '''
    
    _resource_type = Label
    _list_type = LabelList
    
    def list(self, name=None):
        '''Retrieves a list of labels based on the criteria. 

        Args:
            name (str): name of label
        Returns:
            (list): objects meeting filter criteria
        '''
        
        query_list = []
            
        if name:
            name_query = f'name = "{name}"'
            query_list.append(name_query)

        if len(query_list) > 0:
            filter = 'WHERE ' + ' AND '.join(query_list)
        else:
            filter = ''
            
        query = f'SELECT * FROM {self._table_name} {filter}'
            
        return retrieveData(
            client=self._client, 
            query=query,
            resource_type=self._resource_type,
            list_type=self._list_type,
        )