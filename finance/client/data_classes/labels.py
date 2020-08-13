from finance.client.data_classes._base import *
from dataclasses import dataclass, field
import uuid

@dataclass
class Label():
    '''Generic class for labeling of objects'''
    
    name: str
    description: str
    resource_type: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    
class LabelList(GenericList):
    '''List of Type'''