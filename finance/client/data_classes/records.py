from dataclasses import dataclass, field
from finance.client.data_classes._base import *
import pandas as pd
from datetime import datetime
import uuid
from finance.client.utils._database import retrieveData
from finance.client.utils._currency_conversion import get_exchange_rates

@dataclass
class Record():
    '''Individual record for a balance at a point in time for an account'''
    
    date: datetime
    account_id: str
    balance: float
    currency: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    _client_resource_class='records'
    
    def convert_currency(self, currency, date=None, forex_rates=None):
        '''Converts the currency of the Record.
        
        Args:
            currency (str): currency to convert Record to
            date (datetime): defaults to date of record, but can specify specific date
            forex_rates (dict): optional, manually enforce the foreign exchange rates for all currencies
            
        Returns:
            (Record): returns record with designated currency
        '''
        
        if forex_rates:
            self.balance = round(self.balance / forex_rates['rates'][self.currency], 2)
            self.currency = currency
        
        elif date:
            exchange_rates = get_exchange_rates(
                base=currency, 
                date=date
            )
            self.balance = round(self.balance / exchange_rates['rates'][self.currency], 2)
            self.currency = currency
            
        else: # default case with no manual date or forex
                        
            if self.currency != currency:
                exchange_rates = get_exchange_rates(
                    base=currency, 
                    date=self.date
                )
                
                self.balance = round(self.balance / exchange_rates['rates'][self.currency], 2)    
                self.currency = currency
                
        return self

    
class RecordList(GenericList):
    '''List of Records'''
    
    def convert_currency(self, currency, date=None):
        '''Converts the currency of all records to the specified currency
        
        Agrs:
            currency (str): currency to convert to
            date (datetime): date of FOREX rate for currency; defaults to date of record
            
        Returns:
            (RecoordList): list of records with modified currencies / balances
        '''
    
        if date:
            exchange_rates = get_exchange_rates(
                base=currency, 
                date=datetime.date(date)
            )
                        
            for record in self:
                record.convert_currency(currency=currency, forex_rates=exchange_rates)
        
        else:
            for record in self:
                record.convert_currency(currency)
                    
        return self
    
    def to_pandas(self):
        '''Retrieves records in a DataFrame with an option to convert currency.
        
        Args:
            currency (str): currency to convert all records to
        
        Returns:
            (DataFrame): records in a dataframe
        '''
        df = pd.DataFrame([vars(x) for x in self._inner_list])
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            
        return df
    
    
class RecordsAPI(GenericAPI):
    
    _table_name = 'records'
    _table_definition = '''
        CREATE TABLE records (
            id varchar(10) PRIMARY KEY,
            date text NOT NULL,
            account_id varchar(10) NOT NULL,
            balance real NOT NULL,
            currency text NOT NULL,
            FOREIGN KEY (account_id) REFERENCES accounts (id)
        )
    '''
    
    _resource_type = Record
    _list_type = RecordList

    def list(self, account_ids=None, currencies=None):
        '''Retrieves a list of records based on the criteria. 
        List of conditions are treated as `OR`, and multiple conditions are treated as `AND`
        For example:
            (account_id=111 OR account_id=222) AND (currency=USD)

        Args:
            account_ids (list of str): unique ids of the account on the record
            currencies (list of str): currency code for the record
        Returns:
            (list): objects meeting filter criteria
        '''
        
        query_list = []
            
        if account_ids:
            formatted_account_ids = ','.join([f"'{account_id}'" for account_id in account_ids])
            account_id_query = f"account_id IN ({formatted_account_ids})"
            query_list.append(account_id_query)

        if currencies:
            formatted_currencies = ','.join([f"'{curr}'" for curr in currencies])
            currency_query = f"currency IN ({formatted_currencies})"
            query_list.append(currency_query)
            
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
        
    def get_latest(self, account_ids=None):
        '''Retrieves the newest record for each of the accounts provided
        
        Args:
            account_ids (list of str): account ids to retrieve, defaults to all accounts
            
        Returns:
            (RecordList): most recent records on each account
        '''
        
        if account_ids:
            account_filter = 'WHERE account_id IN (' + ','.join([f"'{id}'" for id in account_ids]) + ')'
            
        else:
            account_filter = ''
            
        query = f'''
            SELECT * FROM (
                SELECT account_id, MAX(date) AS date 
                FROM {self._table_name}
                {account_filter} 
                GROUP BY account_id
            ) max_table
            INNER JOIN {self._table_name} full_table
            ON full_table.account_id = max_table.account_id AND full_table.date = max_table.date
        '''
                        
        return retrieveData(
            client=self._client, 
            query=query,
            resource_type=self._resource_type,
            list_type=self._list_type
        )