import sys
sys.path.insert(0,'/Users/jarad/Fake Folder/Python Libraries')

from jb_libraries import *

def get_osh_data(date_start, date_end):

    print('\n== osh_main start ==\n')

    osh_super_main = pd.read_sql(
    '''
    SELECT
    osh.orders_status_history_id,
    DATE(osh.date_added) AS date_added,
    DATE(o.date_purchased) AS date_purchased,
    osh.orders_id,
    LOWER(os.orders_status_name) AS orders_status_name,
    LOWER(o.payment_method) AS payment_method,
    admin_comments
    FROM orders_status_history osh
    JOIN orders_status os ON osh.orders_status_id = os.orders_status_id
    JOIN orders o ON osh.orders_id = o.orders_id
    WHERE DATE(osh.date_added) BETWEEN ' '''+ date_start +''' ' AND ' '''+ date_end +''' '
    ''', db)

    col_fix(osh_super_main)

    min_oid = osh_super_main['orders id'].min()

    ot = pd.read_sql(
    '''
    SELECT
    orders_id,
    value AS order_subtotal
    FROM orders_total
    WHERE class = 'ot_subtotal'
    AND orders_id >= '''+ str(min_oid) +'''
    ''', db)

    col_fix(ot)

    osh_super_main['order subtotal'] = osh_super_main['orders id'].map(dict(zip(ot['orders id'], ot['order subtotal'])))

    osh_main = osh_super_main.copy()
    osh_main.sort_values(['orders id','orders status history id'], inplace = True)
    osh_main.drop_duplicates('orders id', keep = 'last', inplace = True)

    # add some date columns        
    for col in ['date added','date purchased']:
        osh_main[col] = pd.to_datetime(osh_main[col])
        for col2 in ['year and month','year and quarter']:
            osh_main[col2 + ' ' + col.split(' ')[-1]] = jb_dates(osh_main[col], col2)
            
    print('\n== osh_main end ==\n')
    return osh_main