#==========================================================================================
# Load libraries and get dates
#==========================================================================================

import os
tilde = os.path.expanduser('~')

import sys
sys.path.insert(0, tilde + '/Scripts/Fake Folder/Python Libraries')

from jb_libraries import *

from dhl_data import *

def get_shipping_audit_data(date_start, date_end):

    print('\n== shipping audit data start ==\n')

#==========================================================================================
# Orders Data
#==========================================================================================

    adabox_oids = pd.read_sql(
    '''
    SELECT
    orders_id
    FROM subscriptions_history
    WHERE subscriptions_id IN (SELECT subscriptions_id FROM subscriptions WHERE subscriptions_type = 'adabox')
    ''', db)

    col_fix(adabox_oids)
    adabox_oids = adabox_oids['orders id'].tolist()

    o_main = pd.read_sql(
    '''
    SELECT
    DATE(date_purchased) AS date_purchased,
    orders_id,
    LOWER(shipping_module_code) AS shipping_module_code,
    delivery_city,
    delivery_state,
    delivery_country,
    IF(orders_reseller = 0 AND orders_super_reseller = 0,'non reseller','reseller/super') AS customer

    FROM orders

    # keep in replacements, refunds, returns, etc. since we still pay shipping on those
    WHERE orders_status NOT IN (8,9,14,15) #fraud pending/confirmed, voided, fraud-voided
    AND orders_status != 13 # no shipment necessary
    AND DATE(date_purchased) BETWEEN ' '''+ date_start +''' ' AND ' '''+ date_end +''' '
    ''', db)

    col_fix(o_main)

    main = o_main.copy()
    main['date purchased'] = pd.to_datetime(main['date purchased'])    

    for col in ['year and month','year and quarter']:
        main[col] = jb_dates(main['date purchased'], col)

    ot_main = pd.read_sql(
    '''
    SELECT
    orders_id,
    CASE WHEN class = 'ot_shipping' THEN 'service revenue'
    WHEN class = 'ot_ddp' THEN 'ddp revenue'
    WHEN class = 'ot_subtotal' THEN 'subtotal'
    END AS class,
    value
    FROM orders_total
    WHERE class IN ('ot_subtotal','ot_shipping','ot_ddp')
    AND orders_id IN
    (SELECT orders_id FROM orders WHERE DATE(date_purchased) BETWEEN ' '''+ date_start +''' ' AND ' '''+ date_end +''' '
    AND orders_status != 13)
    ''', db)

    col_fix(ot_main)

    ot_main['class'] = ot_main['class'].str.replace('ot_','')
    ot_main['class'] = ot_main['class'].str.strip()
    
    # structure so we can map to main
    df = ot_main.groupby(['orders id','class'])[['value']].sum().unstack(1).fillna(0)
    df.columns = df.columns.droplevel(0)
    df['shipping revenue'] = df[['ddp revenue','service revenue']].sum(1)
    for col in df.columns:
        main[col] = main['orders id'].map(dict(zip(df.index.to_series(), df[col])))    

    main['free shipping'] = np.where(main['shipping revenue'] == 0, 'yes','no')    

    main['delivery country'] = main['delivery country'].str.strip()
    d = {'falkland islands (malvinas)':'falkland islands',
      'macedonia, the former republic of':'macedonia',
      'ontario, canada':'canada',
      'great britain':'united kingdom',
      'virgin islands (u.s.)':'virgin islands'}
    for k,v in d.items():
        main['delivery country'].replace(k,v,inplace = True)

#==========================================================================================
# UPS Data
#==========================================================================================    

    ddp_ls = ['Agri Processing',
            'Broker Fee',
            'Brokerage Fees',
            'Brokerage GST',
            'Ca British Columbia Pst',
            'Ca Customs Hst',
            'Complex Entry',
            'Customs Gst',
            'Customs Warehouse',
            'DGoods Air Inaccessible',
            'Duty Amount',
            'Pst Quebec',
            'QST']

    ups_super_main = pd.read_sql(
    '''
    SELECT
    orders_id,
    charge_description,
    netAmount AS net_amount
    FROM ups_billing
    WHERE orders_id IN
    (SELECT
    orders_id 
    FROM orders
    WHERE DATE(date_purchased) BETWEEN ' '''+ date_start +''' ' AND ' '''+ date_end +''' '
    AND shipping_module_code = 'upsxml')
    ''', db)

    col_fix(ups_super_main)

    ups_super_main['type'] = np.where(ups_super_main['charge description'].isin(ddp_ls), 'ddp charge', 'service charge')
    ups_main = ups_super_main.groupby(['orders id','type'])[['net amount']].sum().unstack(1).fillna(0)
    ups_main.columns = ups_main.columns.droplevel(0)
    ups_main['shipping charge'] = ups_main.sum(1)
    ups_main.reset_index(inplace = True)
    ups_main['shipping module code'] = 'upsxml'    

#==========================================================================================
# DHL data
#==========================================================================================    

    dhl_for_audit = dhl_main[['orders id','service charge','ddp charge','shipping charge']].copy()
    dhl_for_audit['shipping module code'] = 'dhlexpress'

#==========================================================================================
# USPS data
#==========================================================================================    

    usps_main = pd.read_sql(
    '''
    SELECT
    orders_id,
    sl_cost AS shipping_charge
    FROM ship_log
    WHERE orders_id IN
    (SELECT
    orders_id 
    FROM orders
    WHERE DATE(date_purchased) BETWEEN ' '''+ date_start +''' ' AND ' '''+ date_end +''' '
    AND shipping_module_code = 'usps')
    ''', db)

    col_fix(usps_main)

    usps_main['ddp charge'] = 0
    usps_main['service charge'] = usps_main['shipping charge']
    usps_main['shipping module code'] = 'usps'

#==========================================================================================
# Structure it all
#==========================================================================================    

    shipping_main = pd.concat([ups_main, dhl_for_audit, usps_main], sort = True)

    ls = ['service charge','ddp charge','shipping charge']
    for col in ls:
        main[col] = main['orders id'].map(dict(zip(shipping_main['orders id'], shipping_main[col])))

    main['shipping profit'] = main['shipping revenue'] - main['shipping charge']  
    main['shipping profit'] = [np.round(x) for x in main['shipping profit']]
    main['profit loss'] = np.where(main['shipping profit'] < 0, 'yes', 'no')
    main['adabox'] = np.where(main['orders id'].isin(adabox_oids), 'yes', 'no')    

#==========================================================================================
# Missing or removed orders
#==========================================================================================    

    thirty_five_days_ago = str((dt.datetime.now() - pd.DateOffset(days = 35)).date())

    dhl_remove = main[(main['shipping module code'] == 'dhlexpress') # if it's DHL
                  & (main['date purchased'] >= thirty_five_days_ago) # and purchased within the last 35 days
                  & (main['ddp revenue'] > 0) # and we charged DDP
                  & (main['ddp charge'] == 0)].copy() # but the DDP bill from DHL has not yet come

    ls1 = list(o_main['orders id'])
    ls2 = ['upsxml','dhlexpress','usps']
    ls3 = list(shipping_main['orders id'])
    ls4 = list(dhl_remove['orders id'])
    main['missing'] = np.where((main['orders id'].isin(ls1)) # if an OID is in orders table
                          & (main['shipping module code'].isin(ls2)) # and is a UPS/DHL/USPS order
                          & ((~main['orders id'].isin(ls3)) | main['orders id'].isin(ls4)), # and is not in shipping_main OR is in dhl_remove
                            'yes','no')

    # now, where shipping module code NOT IN (ups, usps, dhl), the shipping rev, profit, etc. will be null
    # but these are not missing orders, they just don't have any shipping cost associated with them
    # so fill these nulls with zero
    ls = ['service charge','ddp charge','shipping charge','shipping profit']
    for col in ls:
        main[col] = np.where(~main['shipping module code'].isin(['upsxml','dhlexpress','usps']), 0, main[col])

#==========================================================================================
# Final touches
#==========================================================================================    

    d = {'upsxml':'UPS',
         'usps':'USPS',
         'dhlexpress':'DHL',
         'free':'gift certificate/software',
         '':'employee/other',
         'resellershipping':'reseller shipping',
         '----- no shipping selected -----':'employee/other'}

    for k,v in d.items():
        main['shipping module code'] = main['shipping module code'].replace(k,v)

    for col in main.columns:
        if col in ['year and quarter','shipping module code']:
            pass
        else:
            try:
                main[col] = [x.lower() for x in main[col]]
            except:
                pass    

    main['ddp charge'].fillna(0, inplace = True)

#==========================================================================================
# Check totals
#==========================================================================================    

    c = pd.read_sql(
    '''
    SELECT
    COUNT(*) AS count
    FROM orders 
    WHERE orders_status NOT IN (8,9,14,15,13) # fraud pending/confirmed, voided, fraud-voided
    AND DATE(date_purchased) BETWEEN ' '''+ date_start +''' ' AND ' '''+ date_end +''' '
    ''', db)

    c1 = c['count'].values[0]
    c2 = len(set(main['orders id']))

    if c1 == c2:
        pass
    else:
        raise ValueError('count of orders do not match')
 
    print('\n== shipping audit data end ==\n')

    return main