import requests

def get_exchange_rates(base, date=None, return_currencies=None):
    '''Retrieves FOREX from https://api.exchangeratesapi.io
    
    Args:
        base (str): currency to retrieve exchange rate based on 
        date (str): date to retrieve exchange rate for
        return_currencies (list of str): currencies to return from the API
    
    Returns:
        (dict): rates, base, and date of exchange rate
    '''
    
    params={'base':base}
    
    if date == None:
        url = f'https://api.exchangeratesapi.io/latest'
    else:
        url = f'https://api.exchangeratesapi.io/{date}'
        
    if return_currencies != None:
        params['symbols'] = ','.join(return_currencies)

    response = requests.get(url, params=params)
    exchange_rates = response.json()
    
    return exchange_rates

def convert_currency_dataframe(df, currency):
    '''Consumes DataFrame, retrieves exchanges rates, and adds an additional colums 
        for the converted currency and FOREX
    
    Args:
        df (DataFrame): data frame which much contain columns: 
            'date', 'currency', 'balance'
        currency (str): currency to convert to
        
    Returns:
        (DataFrame): returns same data frame with additional colums for 
            the converted currency and FOREX
    '''
    def convert_currency(row, currency):
        
        if row['currency'] == currency:

            return {f'balance_{currency}': row['balance'],
                    'FOREX':1}

        else:    
            exchange_rates = get_exchange_rates(
                base='USD', 
                date=row['date'].date(), 
                return_currencies=[row['currency']])

            return {f'balance_{currency}': (row['balance'] / exchange_rates['rates'][row['currency']]),
                    'FOREX':exchange_rates['rates'][row['currency']]}
        
    
    df[[f'balance_{currency}', 'FOREX']] = df.apply(
        convert_currency, 
        axis=1, 
        result_type='expand',
        args=(currency,)
    )
    
    return df