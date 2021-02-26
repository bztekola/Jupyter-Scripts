print('\n== jb_google_analytics start ==\n')

import os
tilde = os.path.expanduser('~')

import sys
sys.path.insert(0, tilde + '/Scripts/Fake Folder/Python Libraries')

from jb_libraries import *

from google2pandas import *

conn = GoogleAnalyticsQuery(secrets = './ga-creds/client_secrets.json',
                            token_file_name = './ga-creds/analytics.dat')

def get_ga(account_id, date_start, date_end, dimensions, metrics, filters):
    
    date_ls = pd.date_range(date_start, date_end)
    date_ls = [str(x.date()) for x in date_ls]

    master_ls = []

    for day in date_ls:
        for ix in np.arange(1,1000000,10000):
            query = {'ids':account_id,
                     'dimensions':dimensions,
                     'metrics':metrics,
                     'start_date':day,
                     'end_date':day,
                     'start_index':ix,
                     'max_results':10000,
                     'filters':filters}

            inter_df, metadata = conn.execute_query(**query)
            vals = inter_df.values

            if len(vals) > 0:
                master_ls.append(vals)
            else:
                break

    df = pd.DataFrame(np.concatenate(master_ls), columns = dimensions + metrics)
    df['account id'] = account_id

    cols = []
    for col in df.columns:
        c = re.findall('[a-zA-Z][^A-Z]*', col)
        c = [x.lower() for x in c]
        c = ' '.join(c)
        cols.append(c)
    df.columns = cols

    to_number = []
    for col in metrics:
        c = re.findall('[a-zA-Z][^A-Z]*', col)
        c = [x.lower() for x in c]
        c = ' '.join(c)
        to_number.append(c)    
    
    for col in df.columns:
        if col == 'date':
            df[col] = pd.to_datetime(df[col])
            df['year'] = df[col].dt.year
            df['year and month'] = jb_dates(df[col], 'year and month')
            df['year and quarter'] = jb_dates(df[col], 'year and quarter')
        elif col in to_number:
            df[col] = pd.to_numeric(df[col])
        else:
            try:
                df[col] = df[col].str.lower()
            except:
                pass
    
    return df

print('Your ga function is: get_ga(account_id, date_start, date_end, dimensions, metrics, filters)\n')
print('Beware of sampling!')

print('\n== jb_google_analytics end ==\n')    