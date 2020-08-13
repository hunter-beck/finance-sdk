class Client():
    '''represents an instance of use of the finance tool'''
    
    def __init__(self, config, accounts, account_records, 
                 country_codes, currency_codes):
        '''Initializes instance of the finance tool with static config,
        static data, and account records
        
        Args:
            config (str): path to yaml file with general config information
            accounts (str): path to csv file with account information
            accounts_records (str): path to csv file with historical account information
            country_codes (str): path to csv file with country codes
            currency_codes (str): path to csv file with currency codes
        '''
        
        self._config_file = config
        self._accounts_file = accounts
        self._accounts_records_file = account_records
        self._country_codes_file = country_codes
        self._currency_codes_file = currency_codes

        with open(config) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)    
            self._account_types = config['account_types']
        
        self.account_records = AccountRecordList()
        
        with open(self._accounts_records_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            for row in csv_reader:
                self.account_records.append(AccountRecord(**row))

        self.accounts = AccountList()

        with open(self._accounts_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            for row in csv_reader:
                self.accounts.append(Account(**row))      
        
        self._country_codes = []

        with open(self._country_codes_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            for row in csv_reader:
                self._country_codes.append(row['Alpha-3 code'])

        
        self._currency_codes = []

        with open(self._currency_codes_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
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
            raise AccountValueError(account_record.account_id)
            
        if account_record.currency not in self._currency_codes:
            raise ValueError(f'Currency: {account_record.currency} is not valid. Valid currencies are: {self._currency_codes}')
        
        self.account_records.append(account_record)
        