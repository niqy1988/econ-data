import datetime
import yaml
import requests_cache

import pandas_datareader as pdr

with open('api_keys.yml') as api_keys_config:
    api_keys = yaml.safe_load(api_keys_config)

with open('data_fields.yml') as data_fields_config:
    data_fields = yaml.safe_load(data_fields_config)

session_expire_after = datetime.timedelta(days=3)
session = requests_cache.CachedSession(cache_name='cache', backend='sqlite', expire_after=session_expire_after)

start_date = data_fields['general']['start_date']
end_date = data_fields['general']['end_date']

fred_data = None
for field in data_fields['fred']:
    data_df = pdr.fred.FredReader(field['symbol'], start=start_date, end=end_date, session=session).read()
    data_df.rename(columns={field['symbol']: field['name']}, inplace=True)
    if fred_data is None:
        fred_data = data_df
    else:
        fred_data = fred_data.join(data_df)

print(fred_data)
fred_data.to_pickle('output/fred.pkl')

