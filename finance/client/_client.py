import csv
from finance.client.data_classes.records import Record, RecordList, RecordsAPI
from finance.client.data_classes.accounts import Account, AccountList, AccountsAPI
from finance.client.data_classes.labels import Label, LabelList, LabelsAPI
from pathlib import Path
import os
import csv
import sqlite3
from sqlite3 import OperationalError

class Client():
    '''Represents an instance of use of the finance tool'''
    
    def __init__(self, db_type, server=None, database=None, 
                 username=None, password=None, driver=None, db_path=None):
        '''Initializes instance of the finance client
        '''
        self._db_path = db_path 
        self._db_type = db_type
        
        if db_type == 'sqlite':
                
            self.accounts = AccountsAPI(client=self)
            self.records = RecordsAPI(client=self)
            self.labels = LabelsAPI(client=self)
            
        elif db_type == 'azure-sql':
            self._server = server
            self._database = database
            self._username = username
            self._password = password
            self._driver = driver
                
            self.accounts = AccountsAPI(client=self,)
            self.records = RecordsAPI(client=self)
            self.labels = LabelsAPI(client=self)
            
        self._country_codes_file = Path(__file__).parent / '_resources' / 'country_codes.csv'
        self._currency_codes_file = Path(__file__).parent / '_resources' / 'currency_codes.csv'
        
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