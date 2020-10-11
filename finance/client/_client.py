import csv
from finance.client.data_classes.records import AccountRecord, AccountRecordList, account_records_table_definition
from finance.client.data_classes.accounts import Account, AccountList, accounts_table_definition
from finance.client.data_classes.labels import Label, LabelList, labels_table_definition
from finance.client.utils._database import createTable, updateMultipleRecords
from pathlib import Path
import os
import yaml
import csv
import sqlite3
from sqlite3 import OperationalError

class Client():
    '''Represents an instance of use of the finance tool'''
    
    def __init__(self, db_path=Path('data/db.sqlite3')):
        '''Initializes instance of the finance client
        '''
        
        self._db_path = db_path
        
        db_connection = sqlite3.connect(db_path)
            
        self._data_types = {
            'accounts' : {
                'table_name' : 'accounts',
                'table_definition' : accounts_table_definition,
                'resource_type' : Account,
                'list_type': AccountList
            },
            'account_records' : {
                'table_name' : 'account_records',
                'table_definition' : account_records_table_definition,
                'resource_type' : AccountRecord,
                'list_type': AccountRecordList
            },
            'labels' : {
                'table_name' : 'labels',
                'table_definition' : labels_table_definition,
                'resource_type' : Label,
                'list_type': LabelList
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
            records = getattr(self, data_type)
            
            if len(records) != 0:
                updateMultipleRecords(
                    db_path=self._db_path, 
                    table_name=self._data_types[data_type]['table_name'],
                    records=records
                )    
        
        
    def _read_data_construct_list(self, table_name, table_definition, resource_type, list_type):
        '''Check wether the table exists (create the table if not found 
        and add them to the list object passed to the function.
        
        Args:
            table_name (str): name of the sql table
            table_definition (str): defines the format of the sql table
            resource_type (type): resource type to construct list of
            list_type (type): type of list object which will be constructed
        Returns:
            (List Object): list object with appended data based on object_type
        '''
        
        list_object = list_type()
        
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        try:
            cur.execute(f'SELECT * FROM {table_name}')
        except OperationalError:
            createTable(self._db_path, table_definition)
        rows = cur.fetchall()
        cur.close()
        con.close()
        
        for row in rows:
            values = list(row)
            keys = row.keys()
            row_dict = dict(zip(keys,values))
            list_object.append(resource_type(**row_dict))
        
        return list_object