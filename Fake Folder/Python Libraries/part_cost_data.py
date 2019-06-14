print('\n== part_cost_data start ==\n')

from jb_libraries import *
now = str(dt.datetime.now())[:-7]

#=========================
# parts and skus cost
#=========================

parts_and_skus_cost = pd.read_sql(
'''
SELECT
sku_date_modified,
sku_status,
part_id,
k.sku_id,
IF(pm.latest_cost = 0 OR k.sku_outsourced_assembly = 1, pm.consigned_latest_cost, pm.latest_cost) AS latest_cost,
CASE WHEN part_id = 0 THEN 'sku'
WHEN part_id > 0 AND part_id IN (SELECT part_id FROM parts WHERE products_stripes = 1) THEN 'stripe'
WHEN part_id > 0 AND part_id NOT IN (SELECT part_id FROM parts WHERE products_stripes = 1) THEN 'part'
ELSE 'check this' END AS 'type'
FROM skus k
LEFT JOIN products_manufacturing pm ON k.sku_id = pm.sku_id
''', db)

col_fix(parts_and_skus_cost)

#=========================
# combos cost
#=========================

pts = pd.read_sql(
'''
SELECT
part_id,
contains_part_id,
pts_quantity
FROM products_to_stuff pts
WHERE part_id > 0
''', db)

col_fix(pts)

# we need to map sku_ids to the contains_part_id
# but some part_ids have multiple skus
# here we get either the working sku or the most recent sku per part id
ls = []
for part_id in set(parts_and_skus_cost[parts_and_skus_cost['part id'] > 0]['part id'].tolist()):
    df = parts_and_skus_cost[parts_and_skus_cost['part id'] == part_id].copy()
    if len(df[df['sku status'] == 'working']) == 1: # if there is one working part_id, keep it
        df2 = df[df['sku status'] == 'working']
        ls.append(list(df2.iloc[0]))
    else: # if there is none or more than one, keep the most recent part_id
        df.sort_values('sku date modified', inplace = True)
        ls.append(list(df.iloc[-1]))
        
skus_to_parts = pd.DataFrame(columns = parts_and_skus_cost.columns, data = ls)        

# map skus data
cols = skus_to_parts.columns.tolist()
cols.remove('part id')

for c in cols:
    pts[c] = pts['contains part id'].map(dict(zip(skus_to_parts['part id'], skus_to_parts[c])))
    
# lates_cost is cost per unit, some combos have more than one unit of some part
# so get the latest_cost2
pts['latest cost2'] = pts['pts quantity'] * pts['latest cost']

# group_by combo_part_id, sum modified cost
combos_cost = pts.groupby('part id', as_index = False)[['latest cost2']].sum()
combos_cost.rename(columns = {'latest cost2':'latest cost'}, inplace = True)

# make some columns
combos_cost['type'] = 'combo'

#=========================
# stripes cost
#=========================

stripes_cost = pd.read_sql(
'''
SELECT
part_id,
products_stripe_of,
products_stripe_qty
FROM parts
WHERE products_stripes = 1
''', db)

col_fix(stripes_cost)

# map sku_id and latest_cost
stripes_cost['sku id'] = stripes_cost['part id'].map(dict(zip(stripes_cost['part id'], skus_to_parts['sku id'])))
stripes_cost['latest cost'] = stripes_cost['products stripe of'].map(dict(zip(skus_to_parts['part id'], skus_to_parts['latest cost'])))

# get the cost per reel
for_cost = stripes_cost.sort_values(['products stripe of','products stripe qty'])
for_cost.drop_duplicates('products stripe of', keep = 'last', inplace = True)
for_cost['cost per reel'] = for_cost['latest cost']/for_cost['products stripe qty']

stripes_cost['cost per reel'] = stripes_cost['products stripe of'].map(dict(zip(for_cost['products stripe of'], for_cost['cost per reel'])))

# get the cost_per_reel * qty
stripes_cost['latest cost2'] = stripes_cost['cost per reel'] * stripes_cost['products stripe qty']
stripes_cost.drop(['latest cost',
                   'products stripe of',
                   'products stripe qty',
                   'cost per reel'],1,inplace = True)
stripes_cost.rename(columns = {'latest cost2':'latest cost'}, inplace = True)
stripes_cost['type'] = 'stripe'

#=========================
# concat
#=========================

# drop stripes parts from parts_and_skus
parts_and_skus_cost.drop(parts_and_skus_cost[parts_and_skus_cost['part id'].isin(stripes_cost['part id'].tolist())].index, inplace = True)
parts_and_skus_cost.reset_index(drop = True, inplace = True)

all_cost = pd.concat([parts_and_skus_cost, combos_cost, stripes_cost], sort = True)

all_cost['sku id'].fillna(0, inplace = True)
all_cost['sku status'].fillna('working', inplace = True)
all_cost['sku date modified'].fillna(now, inplace = True)

#=========================
# check and clean up
#=========================

n = all_cost[all_cost.isnull().any(1)]
print('{:,.0f} nulls'.format(len(n)))

ls = ['part id','sku id']
for col in ls:
    all_cost[col] = [int(x) for x in all_cost[col]]
    
#=========================
# get cost by part
#=========================

cols = all_cost.columns.tolist()

ls = []
for part_id in set(all_cost[all_cost['part id'] > 0]['part id']):
    df = all_cost[all_cost['part id'] == part_id].copy()
    if len(df[df['sku status'] == 'working']) == 1: # if there is one working part_id, keep it
        df2 = df[df['sku status'] == 'working']
        ls.append(list(df2.iloc[0]))
    else: # if there is none or more than one, keep the most recent part_id
        df.sort_values('sku date modified', inplace = True)
        ls.append(list(df.iloc[-1]))
        
all_cost_by_part = pd.DataFrame(columns = cols, data = ls)     

#=========================
# get mean or median stripe margin
#=========================        
    
s = all_cost_by_part[all_cost_by_part['type'] == 'stripe']['part id'].tolist()

pr = pd.read_sql(
'''
SELECT
part_id,
products_price
FROM parts
WHERE part_id IN '''+ str(tuple(s)) +'''
''', db)

col_fix(pr)

pr['cost'] = pr['part id'].map(dict(zip(all_cost_by_part['part id'], all_cost_by_part['latest cost'])))
pr['margin'] = (pr['products price'] - pr['cost'])/pr['products price']

mean_stripe_margin = pr['margin'].median()

print('the mean_stripe_margin is {:,.2f}%'.format(mean_stripe_margin * 100))

#=========================
# get MSRP
#=========================        

msrp = pd.read_sql(
'''
SELECT
part_id,
products_price AS msrp
FROM parts
''', db)

col_fix(msrp)

all_cost['msrp'] = all_cost['part id'].map(dict(zip(msrp['part id'], msrp['msrp'])))
all_cost['gross profit'] = all_cost['msrp'] - all_cost['latest cost']

s = list(set(all_cost[all_cost['msrp'].isnull()]['type']))
print('\nthe part types with no MSRP are/is: {}'.format(s))

#=========================
# get overall gross profit margin
#=========================    

one_year_ago = str((pd.to_datetime(now) - pd.DateOffset(days = 365)).date())

op = pd.read_sql(
'''
SELECT
part_id,
op.orders_id
FROM orders_products op
JOIN orders o ON op.orders_id = o.orders_id
AND DATE(date_purchased) BETWEEN ' '''+ one_year_ago +''' ' AND ' '''+ now +''' '
AND orders_reseller = 0
AND orders_super_reseller = 0
''', db)

col_fix(op)

for_avg = all_cost[all_cost['part id'].isin(list(set(op['part id']))) 
                   & (all_cost['sku status'] == 'working')
                   & (all_cost['msrp'] > 0)]
gp = for_avg['gross profit'].sum()
r = for_avg['msrp'].sum()

avg_gp_m = gp/r

print('\navg gross profit for:\nparts, combos, and stripes\nwhich have been purchased within the last year\nwhose sku_status equals "working"\nwhose msrp is greater than zero\nwhich were bought by non resellers\nis {:,.2f}%'.format(avg_gp_m * 100))

neg = for_avg[for_avg['gross profit'] < 0]
print('\nthe parts with negative gross profit are: {}'.format(list(neg['part id'])))

print('\nyour dfs are: all_cost (cost on the sku level) and all_cost_by_part (cost on the part level)')
print('\n== part_cost_data end ==\n')