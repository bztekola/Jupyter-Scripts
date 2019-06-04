#===== import libraries
import sys
sys.path.insert(0,'/Users/jarad/Scripts/Fake Folder Copy/Python Libraries')

from jb_libraries import *
import glob 

#===== get all CSVs
path = r'/Users/jarad/Scripts/Fake Folder Copy/Shipping/Recurring/Shipping Audit/DHL Invoices'
all_files = glob.glob(path + '/*.csv')
frame = pd.DataFrame()
list_ = []

for file_ in all_files:
    df = pd.read_csv(file_, index_col = None, header = 0)
    list_.append(df)
    
dhl_super_main = pd.concat(list_)   
dhl_super_main.drop_duplicates(inplace = True)

#===== fix columns
dhl_super_main.columns = [x.lower() for x in dhl_super_main]
dhl_super_main.columns = dhl_super_main.columns.str.replace('/','')
dhl_super_main.rename(columns = {'shipment reference 1':'orders id',
                               'dest name':'delivery city',
                               'dest country name':'delivery country',
                               'proof of delivery datetime':'proof of delivery date/time'}, inplace = True)

#===== fix orders id col
dhl_super_main['orders id'] = pd.to_numeric(dhl_super_main['orders id'], errors = 'coerce', downcast = 'integer')

def oid_fix(x):
    if x['orders id'] == 0 or pd.isnull(x['orders id']):
        return x['shipment number']       
    else:
        return x['orders id']
    
dhl_super_main['orders id'] = dhl_super_main.apply(oid_fix, axis = 1)
dhl_super_main['orders id'] = [int(x) for x in dhl_super_main['orders id']]

#===== fix weight charge
dhl_super_main['weight charge'].fillna(0, inplace = True)

#===== drop any column with all zeros
dhl_super_main = dhl_super_main.loc[:, (dhl_super_main != 0).any(axis = 0)]

#===== create "charges df"
charges_col = int((len(dhl_super_main.columns[dhl_super_main.columns.str.contains('xc')]) - 1)/3)

charges01 = pd.DataFrame()
for i in range(charges_col):
    df = dhl_super_main.groupby(['orders id','xc' + str(i + 1) + ' name'])[['xc' + str(i + 1) + ' charge']].sum().unstack(1).fillna(0)
    df.columns = df.columns.droplevel(0)
    
    charges01 = charges01.append(df, ignore_index = False, sort = False)

charges01.reset_index(inplace = True)    
charges01.columns = [str(x).lower() for x in charges01.columns]
charges01.fillna(0, inplace = True)
charges01 = charges01.loc[:, (charges01 != 0).any(axis = 0)].copy()
charges01 = charges01.loc[ (charges01 != 0).any(axis = 1), :].copy()

#===== consolidate columns
ddp_cols01 = charges01.columns[charges01.columns.str.contains('import|export')].tolist()
ddp_cols = list(set(ddp_cols01)) # to remove dupes

permits_cols = ['obtaining permits & licenses','obtaining permits &amp; licenses']
merch_cols = ['merchandise process','merchandise processing']

charges01['ddp charge'] = charges01[ddp_cols].sum(1)
charges01.drop(ddp_cols, 1, inplace = True)

charges01['obtaining permits'] = charges01[permits_cols].sum(1)
charges01.drop(permits_cols, 1, inplace = True)

charges01['merch processing'] = charges01[merch_cols].sum(1)
charges01.drop(merch_cols, 1, inplace = True)

charges01['ship value protect'] = charges01['shipment value protection'].sum(1)
charges01.drop('shipment value protection', 1, inplace = True)

charges02 = charges01.groupby('orders id', as_index = False).sum()

#===== merge "weight charge"
charges03 = pd.merge(charges02,
                     dhl_super_main.groupby('orders id', as_index = False)[['weight charge']].sum(),
                     how = 'outer',
                     on = 'orders id')

charges03.fillna(0, inplace = True)

#===== check totals
if np.abs(charges03.set_index('orders id').sum(1).sum() - dhl_super_main['total charge'].sum()) < 1:
    pass
else:
    raise ValueError('DHL total charges do not match')

#===== get totals
charges03['shipping charge'] = charges03.loc[:,(charges03.columns != 'orders id')].sum(1)
charges03['service charge'] = charges03['shipping charge'] - charges03['ddp charge']

#===== isolate important columns
charges04 = charges03[['orders id','service charge','ddp charge','shipping charge']].copy()

#===== get other attributes
cols = ['delivery city',
        'delivery country',
        'orders id',
        'proof of delivery date/time',
        'senders city',
        'senders country',
        'senders name',
        'shipment date',
        'shipment number',
        'weight',
        'weight unit']

dhl_main = pd.merge(charges04,
                   dhl_super_main[cols].drop_duplicates('orders id'),
                   how = 'outer',
                   on = 'orders id')

#===== check totals
if np.abs(dhl_main['shipping charge'].sum() - dhl_super_main['total charge'].sum()) < 1:
    pass
else:
    raise ValueError('DHL shipping charges do not match')
    
#===== run queries for flags
orders_total_main = pd.read_sql(
'''
SELECT
DATE(o.date_purchased) AS date,
DATE_FORMAT(o.date_purchased, '%Y-%m') AS 'year and month',
ot1.orders_id AS 'orders id',
ot2.value AS 'shipping revenue',
ot3.value AS 'ddp'
FROM orders_total ot1
LEFT JOIN orders_total ot2 ON ot1.orders_id = ot2.orders_id
AND ot2.class = 'ot_shipping'
LEFT JOIN orders_total ot3 ON ot1.orders_id = ot3.orders_id
AND ot3.class = 'ot_ddp'
LEFT JOIN orders o ON ot1.orders_id = o.orders_id
WHERE ot1.orders_id IN (SELECT orders_id FROM orders WHERE shipping_module_code = 'dhlexpress')
AND ot1.class = 'ot_subtotal'
''', db)

orders_total_main.fillna(0, inplace = True)

#===== flag
dhl_main['free shipping'] = np.where(dhl_main['orders id'].isin(orders_total_main['orders id'][orders_total_main['shipping revenue'] == 0].tolist()), 'yes','no')
   
dhl_main['incoming/outgoing'] = np.where(dhl_main['orders id'].isin(orders_total_main['orders id'].tolist()),
                                         'outgoing',
                                         'incoming')

dhl_main['ddp'] = np.where(dhl_main['orders id'].isin(orders_total_main['orders id'][orders_total_main['ddp'] > 0].tolist()), 'yes','no')
dhl_main['date'] = dhl_main['orders id'].map(dict(zip(orders_total_main['orders id'], orders_total_main['date'])))
dhl_main['date'].fillna(dhl_main['shipment date'], inplace = True)
dhl_main['date'] = pd.to_datetime(dhl_main['date'])
dhl_main['year and month'] = dhl_main['orders id'].map(dict(zip(orders_total_main['orders id'], orders_total_main['year and month'])))

def date_fix(x):
    if pd.isnull(x['year and month']):
        return str(x['date'])[:7]
    else:
        return x['year and month']
dhl_main['year and month'] = dhl_main.apply(date_fix, axis = 1)

#===== make pretty, kind of, lol
for col in dhl_main.columns:
    try:
        dhl_main[col] = dhl_main[col].str.lower()
    except:
        pass    
    
#===== check totals
if np.abs(dhl_main['shipping charge'].sum() - dhl_super_main['total charge'].sum()) < 1:
    pass
else:
    raise ValueError('DHL shipping charges do not match')