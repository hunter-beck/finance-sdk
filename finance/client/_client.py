import csv
from finance.client.data_classes.records import *
from finance.client.data_classes.accounts import *
from pathlib import Path
import os
import yaml
import csv

class Client():
    '''represents an instance of use of the finance tool'''
    
    def __init__(self, config=Path('config.yml'), data_directory=Path('data')):
        '''Initializes instance of the finance client
        '''
        
        if not data_directory.is_dir():
            data_directory.mkdir(parents=True)
        
        self._config_file = config
        self._accounts_file = data_directory / 'accounts.csv'
        self._accounts_records_file = data_directory / 'account_records.csv'
        self._country_codes_file = Path(__file__).parent / '_resources' / 'country_codes.csv'
        self._currency_codes_file = Path(__file__).parent / '_resources' / 'currency_codes.csv'

        with open(self._config_file) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)    
            self._account_types = config['account_types']
        
        self.account_records = AccountRecordList()
        
        if self._accounts_records_file.exists():
            with open(self._accounts_records_file, mode='r') as csv_file:
                if csv_file.readable():
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        self.account_records.append(AccountRecord(**row))
                        
        else:
            with open(self._accounts_records_file, mode='w') as csv_file:
                fieldnames = ['datetime','account_id','balance','currency','id']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()

        self.accounts = AccountList()

        if self._accounts_file.exists():
            with open(self._accounts_file, mode='r') as csv_file:
                if csv_file.readable():
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        self.accounts.append(Account(**row))      
                        
        else:
            with open(self._accounts_file, mode='w') as csv_file:
                fieldnames = ['id','name','type','country_code']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
        
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
        