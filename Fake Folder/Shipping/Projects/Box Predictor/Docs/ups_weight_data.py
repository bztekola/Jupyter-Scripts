# get weights for all of 2018
weight_main = pd.read_sql(
'''
SELECT

DATE(date_purchased) AS date_purchased,
DATE_FORMAT(date_purchased, '%Y-%m') AS year_and_month,
ups.orders_id,
tracking,
LOWER(charge_description) AS charge_description,
entered_weight,
billed_weight

FROM orders o

LEFT JOIN ups_billing ups ON o.orders_id = ups.orders_id

WHERE DATE(date_purchased) BETWEEN '2018-01-01' AND '2018-07-31' 
AND shipping_module_code = 'upsxml'
AND shippingcost > 0 # exclude free shipping
AND ups.orders_id IS NOT NULL
AND ups.billed_weight > 0
''', db)

# this function replaces the underscore in the column header with a space
col_fix(weight_main)

# find nulls, any and all
print('{} null(s)'.format(np.sum(weight_main.isnull().any(1))))

# get rid of the words "residential" and "commercial"
weight_main['charge description'] = weight_main['charge description'].str.replace('residential|commercial','')

# get rid of "returns", "delivery intercepts", and "undeliverable"
ls = ['return','intercept','undeliverable']
weight_main.drop(weight_main[weight_main['charge description'].str.contains('|'.join(ls))].index, inplace = True)

# view all the charge descriptions and their counts
print('\ncharge description counts:\n')
print(weight_main['charge description'].value_counts())

# groupby year/month and tracking
# grouping by trackingn and not OID ensures that we capture multi-box shipments
# get the min entered weight per tracking
# get the max billed weight per tracking
    # if there was a shipping charge correction,
    # then this max weight is the final billed weight value after this correction
weight = weight_main.groupby(['year and month','tracking'], as_index = False).agg({'entered weight':'min',
                                                                                    'billed weight':'max'})
# map OIDs to tracking
weight['orders id'] = weight['tracking'].map(dict(zip(weight_main['tracking'], weight_main['orders id'])))

# flag OIDs for shipping charge corrections
ls = weight_main[weight_main['charge description'].str.contains('shipping charge correction')]['tracking'].tolist()
weight['shipping charge correction'] = np.where(weight['tracking'].isin(ls), 'yes', 'no')

# get the weight difference
weight['weight difference'] = weight['billed weight'] - weight['entered weight']

# get shipping revenue
ship_revenue = pd.read_sql(
'''
SELECT
ot1.orders_id,
ot1.value + IFNULL(ot2.value, 0) AS shipping_revenue
FROM orders_total ot1
LEFT JOIN orders_total ot2 ON ot1.orders_id = ot2.orders_id
AND ot2.class = 'ot_ddp'
WHERE ot1.class = 'ot_shipping'
AND ot1.orders_id IN '''+ str(tuple(weight_main['orders id'].tolist())) +'''
''', db)

col_fix(ship_revenue)

# get shipping charges
ship_charge = pd.read_sql(
'''
SELECT
orders_id,
SUM(netAmount) AS shipping_charge
FROM ups_billing
WHERE orders_id IN '''+ str(tuple(weight_main['orders id'].tolist())) +'''
GROUP BY orders_id
''', db)

col_fix(ship_charge)

# merge the two on OID
df = pd.merge(ship_revenue, ship_charge, on = 'orders id')

# flag overcharges
df['overcharge'] = np.where(df['shipping revenue'] < df['shipping charge'], 'yes','no')
ls = df[df['overcharge'] == 'yes']['orders id'].tolist()

# map this result to your weight data
# note that the data in the "weight" df is by tracking,
    # so there are dupe OIDs,
    # that's why we map the yes/no flag and not the actual rev/charge amounts
weight['overcharge'] = np.where(weight['orders id'].isin(ls), 'yes', 'no')

#===== summary and stats
t = len(weight)
m1 = weight['year and month'].min()
m2 = weight['year and month'].max()
print('\n{:,.0f} total boxes considered\nfrom the months {} to {}'.format(t, m1, m2)) # recall that the weight df is by tracking, not by OID

print('\nWhere entered weight is zero')
val1 = len(weight[(weight['entered weight'] == 0) & (weight['billed weight'] > 0)])
print('{:,.2f}%\nor {:,.0f} boxes'.format(val1/t * 100, val1))

print('\nWhere entered weight is greater than zero AND there is a weight difference')
val1 = len(weight[(weight['entered weight'] > 0) & (weight['weight difference'] > 0)])
print('{:,.2f}%\nor {:,.0f} boxes'.format(val1/t * 100, val1))

print('\nWhere entered weight is greater than zero AND there is a weight difference AND there was an overcharge')
val1 = len(weight[(weight['entered weight'] > 0) & (weight['weight difference'] > 0) & (weight['overcharge'] == 'yes')])
print('{:,.2f}%\nor {:,.0f} boxes'.format(val1/t * 100, val1))

print('\nCount of Shipping Charge Corrections')
val1 = len(weight[weight['shipping charge correction'] == 'yes'])
print('{:,.2f}%\nor {:,.0f} boxes'.format(val1/t * 100, val1))

print('\nCount of Shipping Charge Corrections AND Overcharges')
val1 = len(weight[(weight['shipping charge correction'] == 'yes') & (weight['overcharge'] == 'yes')])
print('{:,.2f}%\nor {:,.0f} boxes'.format(val1/t * 100, val1))

#===== to excel
title = 'UPS Weight Data from {} to {}'.format(m1, m2)
writer = pd.ExcelWriter(title + '.xlsx', engine = 'xlsxwriter')
weight.to_excel(writer, 'data', index = False)
writer.save()