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
        
        self._accounts_file = data_directory / 'accounts.csv'
        self._accounts_records_file = data_directory / 'account_records.csv'
        self._labels_file = data_directory / 'labels.csv'
        self._country_codes_file = Path(__file__).parent / '_resources' / 'country_codes.csv'
        self._currency_codes_file = Path(__file__).parent / '_resources' / 'currency_codes.csv'
            
        self.labels = self._read_data_construct_list(
            file_path=self._labels_file, 
            field_names=['id','name','description', 'resource_type'],
            resource_type=Label
        )
            
        self.account_records = self._read_data_construct_list(
            file_path=self._accounts_records_file,
            field_names=['datetime','account_id','balance','currency','id'],
            resource_type=AccountRecord
        )
        

        self.accounts = self._read_data_construct_list(
            file_path=self._accounts_file,
            field_names=['id','name','type','country_code', 'label_id'],
            resource_type=Account
        )
        
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
        
        if account.type not in self._account_types:
            raise ValueError(f'Account type: {account.type} is not valid. Options are: {self._account_types}')
            
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
        
        
    def _read_data_construct_list(self, file_path, field_names, resource_type):
        '''Check wether the file exists (create the file if not found based on field_names
        and add them to the list object passed to the function.
        
        Args:
            file_path (Path): path of the csv file to attempt reading 
            field_names (list of str): field names for the csv file read    
            list_object (GenericList and inherited classes): list object to append data to
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