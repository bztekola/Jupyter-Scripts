print('\n== jb_google_analytics start ==\n')

import sys
sys.path.insert(0,'/Users/jarad/Fake Folder/Python Libraries/')

from jb_libraries import *
from google2pandas import *

conn = GoogleAnalyticsQuery(secrets = './ga-creds/client_secrets.json',
                            token_file_name = './ga-creds/analytics.dat')

def to_df(x):
    x, metadata = conn.execute_query(**x)
    x = pd.DataFrame(x)
    return x

def get_ga(account_id, date_start, date_end, dimensions, metrics):
    
    date_ls = pd.date_range(date_start, date_end)
    date_ls = [str(x.date()) for x in date_ls]

    df = pd.DataFrame()

    for day in date_ls:
        for ix in np.arange(1,1000000,10000):
            query = {
                'ids':account_id,
                'dimensions':dimensions,
                'metrics':metrics,
                'start_date': day,
                'end_date': day,
                'start_index':ix,
                'max_results':10000,
                'samplingLevel':'HIGHER_PRECISION'}

            query_df = to_df(query)
            query_df['accountId'] = account_id
            
            if query_df.empty == False:
                df = df.append(query_df, ignore_index = True)
            else:
                break
        
    df.reset_index(drop = True, inplace = True)    
    
    cols = []
    for col in df.columns:
        c = re.findall('[a-zA-Z][^A-Z]*', col)
        c = [x.lower() for x in c]
        c = ' '.join(c)
        cols.append(c)

    df.columns = cols
    
    for col in df.columns:
        if col == 'date':
            df[col] = pd.to_datetime(df[col])
        try:
            df[col] = df[col].str.lower()
        except:
            pass

    return df

print('Your ga function is: get_ga(account_id, date_start, date_end, dimensions, metrics\n')
print('Note that with this API there is no way to avoid sampling, so averages should be used rather than the actual numbers returned.')

print('\n== jb_google_analytics end ==\n')    