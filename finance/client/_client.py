import csv
from finance.client.data_classes.records import *
from finance.client.data_classes.accounts import *
from finance.client.data_classes.labels import *
from pathlib import Path
import os
import yaml
import csv

class Client():
    '''Represents an instance of use of the finance tool'''
    
    def __init__(self, data_directory=Path('data')):
        '''Initializes instance of the finance client
        '''
        
        if not data_directory.is_dir():
            data_directory.mkdir(parents=True)
            
        self._data_types = {
            'accounts' : {
                'file_path' : data_directory / 'accounts.csv',
                'field_names' : ['id','name','label_id','country_code', 'label_id'],
                'resource_type' : Account
            },
            'account_records' : {
                'file_path' : data_directory / 'account_records.csv',
                'field_names' : ['datetime','account_id','balance','currency','id'],
                'resource_type' : AccountRecord
            },
            'labels' : {
                'file_path' : data_directory / 'labels.csv',
                'field_names' : ['id','name','description', 'resource_type'],
                'resource_type' : Label
            }
        }
        
        self._country_codes_file = Path(__file__).parent / '_resources' / 'country_codes.csv'
        self._currency_codes_file = Path(__file__).parent / '_resources' / 'currency_codes.csv'
        
        for data_type, type_properties in self._data_types.items():
                    
            setattr(self, data_type, self._read_data_construct_list(**type_properties))
        
        self._country_codes = []

        with open(self._country_codes_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                self._country_codes.append(row['alpha3_code'])

        
        self._currency_codes = []

        with open(self._currency_codes_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                self._currency_codes.append(row['alpha_code'])

                    
    def addAccount(self, account):
        '''Add a new account to the client
        
        Args:
            account (Account): banking, invetment or other account
            
        '''
            
        if account.country_code not in self._country_codes:
            raise ValueError(f'Country code: {account.country_code} is not valid. Options are: {self._country_codes}')
            
        self.accounts.append(account)

        
    def addRecord(self, account_record):
        '''Add a new Account Record to the client
        
        Args:
            account_record (AccountRecord): balance on a given date for a given account
        '''
        
        account_ids = []
        
        for account in self.accounts:
            account_ids.append(account.id)
        
        if account_record.account_id not in account_ids:
            raise ValueError(account_record.account_id)
            
        if account_record.currency not in self._currency_codes:
            raise ValueError(f'Currency: {account_record.currency} is not valid. Valid currencies are: {self._currency_codes}')
        
        self.account_records.append(account_record)
        
    
    def writeBack(self, data_types='All'):
        '''Writes data stored temporarily on the client back to the source data files. Optionally, can select
        a subset of data_types to write back
        
        Args:
            data_types ('All' or list of str): 
                'All': all available data_types will be written to respective source files
                list subset of available data_types
        '''
        
        if data_types == 'All':
            data_types = self._data_types.keys()
        
        for data_type in data_types:
            data = getattr(self, data_type)
            df = data.to_pandas()
            df.to_csv(
                path_or_buf=self._data_types[data_type]['file_path'],
                index=False,
            )
        
        
    def _read_data_construct_list(self, file_path, field_names, resource_type):
        '''Check wether the file exists (create the file if not found based on field_names
        and add them to the list object passed to the function.
        
        Args:
            file_path (Path): path of the csv file to attempt reading 
            field_names (list of str): field names for the csv file read    
            resource_type (type): resource type to construct list of
        
        Returns:
            (List Object): list object with appended data based on object_type
        '''
        if resource_type == Account:
            list_object = AccountList()
        elif resource_type == AccountRecord:
            list_object = AccountRecordList()
        elif resource_type == Label:
            list_object = LabelList()
        else:
            raise TypeError(f'Object Type: {resource_type} not recognized')
        
        if file_path.exists():
            with open(file_path, mode='r') as csv_file:
                if csv_file.readable():
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                            list_object.append(resource_type(**row))
                        
        else:
            with open(file_path, mode='w') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=field_names)
                writer.writeheader()
                
        return list_object