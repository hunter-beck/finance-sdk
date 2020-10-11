import csv
from finance.client.data_classes.records import Record, RecordList, RecordsAPI
from finance.client.data_classes.accounts import Account, AccountList, AccountsAPI
from finance.client.data_classes.labels import Label, LabelList, LabelsAPI
from finance.client.utils._database import createTable, updateMultipleRecords
from pathlib import Path
import os
import csv
import sqlite3
from sqlite3 import OperationalError

class Client():
    '''Represents an instance of use of the finance tool'''
    
    def __init__(self, db_path=Path('data/db.sqlite3')):
        '''Initializes instance of the finance client
        '''
        
        self._db_path = db_path
        
        self._country_codes_file = Path(__file__).parent / '_resources' / 'country_codes.csv'
        self._currency_codes_file = Path(__file__).parent / '_resources' / 'currency_codes.csv'
            
        self.accounts = AccountsAPI(db_path=self._db_path)
        self.records = RecordsAPI(db_path=self._db_path)
        self.labels = LabelsAPI(db_path=self._db_path)
        
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