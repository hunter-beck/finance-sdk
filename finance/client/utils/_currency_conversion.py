import requests
from datetime import datetime

def get_exchange_rates(base, date=None, return_currencies=None):
    '''Retrieves FOREX from https://api.exchangeratesapi.io
    
    Args:
        base (str): currency to retrieve exchange rate based on 
        date (datetime): date to retrieve exchange rate for
        return_currencies (list of str): currencies to return from the API
    
    Returns:
        (dict): rates, base, and date of exchange rate
    '''
    
    params={'base':base}
    
    if date == None:
        url = f'https://api.exchangeratesapi.io/latest'
    else:
        iso_date = datetime.isoformat(date).split('T')[0]
        url = f'https://api.exchangeratesapi.io/{iso_date}'
        
    if return_currencies != None:
        params['symbols'] = ','.join(return_currencies)

    response = requests.get(url, params=params)
    exchange_rates = response.json()
    
    return exchange_rates