import sys
sys.path.insert(0,'/Users/jarad/Scripts/Fake Folder Copy/Python Libraries/')

from jb_libraries import *

print('\n== fab_data start ==\n')

#=========================
# Get data
#=========================

sp_actions_main = pd.read_sql(
'''
SELECT
process_id,
sp_id,
IF(spa_qty < spa_qty_reject, spa_qty_reject, spa_qty) AS spa_qty_in,
IF(spa_qty < spa_qty_reject, spa_qty_reject, spa_qty) - spa_qty_reject AS spa_qty_out,
spa_qty_reject,
spa_time_start,
spa_time_end,
spa_admin AS admin
FROM sp_actions
ORDER BY process_id
''', db)

col_fix(sp_actions_main)
sp_actions_main.drop_duplicates(inplace = True)

#=========================

process_class = pd.read_sql(
'''
SELECT
DISTINCT process_class AS process_group
FROM processes
WHERE wo_id IN (SELECT wo_id from work_orders)
''', db)

col_fix(process_class)
ls = process_class['process group'].tolist() + ['processRework']

#=========================

processes_super_main = pd.read_sql(
'''
SELECT
process_id,
IF(rework_id > 0 , 'Rework', process_name) AS process_name,
IF(rework_id > 0, rework_id, wo_id) AS wo_id,
time_start AS process_time_start,
time_end AS process_time_end,
qty_in AS process_qty_in,
qty_out AS process_qty_out,
qty_reject AS process_qty_reject,
process_order,
IF(rework_id > 0, 'rework', process_group) AS process_group,
rework_id
FROM processes
WHERE process_class IN '''+ str(tuple(ls)) +'''
''', db)

col_fix(processes_super_main)

processes_main = processes_super_main.copy()
# qty in can't be less than qty out, so fix that here
processes_main['process qty in'] = np.where(processes_main['process qty in'] < processes_main['process qty out'],
                                            processes_main['process qty out'],
                                            processes_main['process qty in'])
processes_main['process qty reject'] = processes_main['process qty in'] - processes_main['process qty out']

#=========================

wo_main = pd.read_sql(
'''
SELECT
wo_id,
wo_id_string,
wo_date_created,
wo_date_scheduled,
wo_date_active,
wo_date_completed,
wo.sku_id,
k.part_id,
pd.products_name,
wo_qty AS wo_qty_in,
wo_qty_out,
wo_qty - wo_qty_out AS wo_qty_reject,
wo_status,
wo_substatus,
IF(wo_outsourced > 0, 'yes','no') AS wo_outsourced
FROM work_orders wo
LEFT JOIN skus k ON wo.sku_id = k.sku_id
LEFT JOIN products_description pd ON k.part_id = pd.part_id
''', db)

col_fix(wo_main)

#=========================
# Merge and clean
#=========================

main = pd.merge(processes_main, sp_actions_main, how = 'left', on = 'process id').merge(wo_main, how = 'left', on = 'wo id')
main.reset_index(drop = True, inplace = True)

# check to make sure you retain everything after the merge
l1 = len(processes_main['process id'].unique())
l2 = len(main['process id'].unique())

if l1 == l2:
    pass
else:
    raise ValueError('count of process ids do not match')

# some rework ids are such that they are not work order ids
# exclude these weird ones here
main = main[main['wo id'].isin(wo_main['wo id'].tolist())].copy()    

# if the line is a rework, its process order = 0 in the database
# a rework should come after all steps
# fix this here so reworks equal the max process order of the WO plus one
max_ = main.groupby('wo id', as_index = False)[['process order']].max()

def process_order_fix(x):
    if x['process group'] == 'rework':
        process_id = max_[max_['wo id'] == x['wo id']]['process order']
        process_id = process_id.values[0]
        process_id += 1
    else:
        process_id = x['process order']        
    return process_id

main['process order'] = main.apply(process_order_fix, axis = 1)    

main.drop(main[main['wo id'] == 0].index, inplace = True)

# fill in the blanks with "secondary"
main['process group'] = np.where(main['process group'].isin(['none','']), 'secondary', main['process group'])

# clean up the columns
for col in main.columns:
    try:
        main[col] = [x.lower() for x in main[col]]
    except:
        pass

# reorder columns
ls1 = wo_main.columns.tolist()
ls2 = processes_main.columns.tolist()

for x in ['wo id','process id']:
    ls2.remove(x)

ls3 = sp_actions_main.columns.tolist()

cols =  ls1 + ls2 + ls3
main = main[cols].copy()

main.sort_values(['wo id','process order','process id'], inplace = True)

#=========================
# Fix these date nulls and zeros
#=========================

# most date nulls occur when the part is null
no_pn = main[main['part id'].isnull()].copy()

main.drop(main[main['part id'].isnull()].index, inplace = True)
main.reset_index(drop = True, inplace = True)

ls = ['wo date created',
      'wo date scheduled',
      'wo date active',
      'wo date completed',
      'process time start',
      'process time end',
      'spa time start',
      'spa time end']

for col in ls:
    main[col] = main[col].replace('0000-00-00 00:00:00', np.nan)
    main[col] = pd.to_datetime(main[col])
    
# if scheduled occurred in 1969, assume it was scheduled one day before it went active
main['wo date scheduled'] = np.where(main['wo date scheduled'].dt.year == 1969,
                                    main['wo date active'] - pd.DateOffset(days = 1),
                                    main['wo date scheduled'])    

print('WO date nulls even though WO is marked as "completed":')

# save for CSV output
date_nulls = pd.DataFrame()
ls = ['created','scheduled','active','completed']
for col in ls:
    print(col)
    df = main[(main['wo status'] == 'completed') & (main['wo date ' + col].isnull())]
    date_nulls = date_nulls.append(df)
    print(len(df['wo id'].unique()))  

print('these are stored in the "date_nulls" df and have been fixed')

i = date_nulls.columns.get_loc('wo outsourced')
cols = date_nulls.columns[:i + 1]
date_nulls = date_nulls[cols].copy()
date_nulls.drop_duplicates('wo id', inplace = True)
    
# if "wo date active" is null, assume it went active the same day it was scheduled
main['wo date active'] = np.where((main['wo status'] == 'completed') & (main['wo date active'].isnull()),
                                 main['wo date scheduled'],
                                 main['wo date active'])

# if "wo date completed" is null, assume it was completed the same day it went active
main['wo date completed'] = np.where((main['wo status'] == 'completed') & (main['wo date completed'].isnull()),
                                     main['wo date active'],
                                     main['wo date completed'])

#=========================
# Check to make sure the (qty_in) - (qty_reject) = (qty_out)
# Data entry errors made by the user can make this not so
#=========================

ls = ['wo','process','spa']

qty_check_dfs = {}

for x in ls: # for each level (wo, process, and spa) check the qty
    df = main[(main[x + ' qty in'] != main[[x + ' qty out', x + ' qty reject']].sum(1))].dropna()
    qty_check_dfs[x] = df
    
    if len(df) == 0:
        pass
    else:
        raise ValueError(x + ' qty does not match')

#=========================
# Check counts
#=========================        

print('\nThe data shows the following work order counts:')

t1 = len(main['wo id'].unique())
t2 = len(no_pn['wo id'].unique())

t3 = pd.read_sql(
'''
SELECT
COUNT(DISTINCT wo_id) AS count
FROM work_orders
''', db)


if (t1 + t2) == t3['count'][0]:
    pass
else:
    raise ValueError('unique WO id count does not match')
    
print('\nx{:,.0f} unique'.format(t1))

#=========================

c1 = len(main[main['wo status'] == 'completed']['wo id'].unique())
c2 = len(no_pn[no_pn['wo status'] == 'completed']['wo id'].unique())

c3 = pd.read_sql(
'''
SELECT
COUNT(DISTINCT wo_id) AS count
FROM work_orders
WHERE wo_status = 'completed'
''', db)

if (c1 + c2) == c3['count'][0]:
    pass
else:
    raise ValueError('completed WO count does not match')

print('x{:,.0f} completed'.format(c1))    

#=========================

a1 = len(main[main['wo status'] == 'active']['wo id'].unique())
a2 = len(no_pn[no_pn['wo status'] == 'active']['wo id'].unique())

a3 = pd.read_sql(
'''
SELECT
COUNT(DISTINCT wo_id) AS count
FROM work_orders
WHERE wo_status = 'active'
''', db)

if (a1 + a2) == a3['count'][0]:
    pass
else:
    raise ValueError('active WO count does not match')
    
print('x{:,.0f} open'.format(a1))

#=========================

p1 = len(main[main['wo status'] == 'pending']['wo id'].unique())
s = np.sum([a1,c1,p1])

if s == t1:
    pass
else:
    raise ValueError('pending WO count does not match')
    
print('x{:,.0f} pending'.format(p1))

if t1 - c1 - a1 != p1:
    raise ValueError('WO counts do not match')

#=========================
# Create custom week that starts on Friday and ends on Thursday
# The department wants this report each Friday morning for the week previous, Friday to Thursday.
#=========================    

# get min and max dates of data
ls = ['wo date created',
      'wo date scheduled',
      'wo date active',
      'wo date completed',
      'process time start',
      'process time end',
      'spa time start',
      'spa time end']

d1 = main[ls].min().min()
d2 = main[ls].max().max()

# create a date range
weeks = pd.DataFrame(pd.date_range(d1,d2))
weeks.columns = ['date']

# get day name
weeks['day name'] = weeks['date'].dt.day_name()

# if it's a thursday, make this date your "week ending"
weeks['week ending'] = np.where(weeks['day name'] == 'Thursday', weeks['date'].astype(str), np.nan)

# not backfill the nulls, now you have all of your week_endings
weeks['week ending'].fillna(method = 'backfill', inplace = True)
weeks.dropna(inplace = True)

# we want to be able to groupby year and month and year_and_week, so create those columns here
for col in ls:
    
    # split column title
    split = col.split(' ')
    
    # make year_and_month, so the first will be called "wo year and month active"
    new_col = split[0] + ' year and month ' + split[-1]
    main[new_col] = [str(x)[:7] for x in main[col]]
    
    # get week ending
    new_col2 = split[0] + ' week ' + split[-1]
    main[new_col2] = main[col].dt.date.map(dict(zip(weeks['date'], weeks['week ending'])))

ls = ['wo date created',
      'wo date scheduled',
      'wo date active',
      'wo date completed']

for col in ls:
    main[col] = main[col].dt.date
    main[col] = pd.to_datetime(main[col])

#=========================
# where products name is null, because there is no part id, fill it in with the sku name
#=========================    

s = main[(main['sku id'] > 0) & (main['products name'].isnull())]['sku id'].tolist()

k = pd.read_sql(
'''
SELECT
sku_id,
sku_name
FROM skus
WHERE sku_id IN '''+ str(tuple(s)) +'''
''', db)
col_fix(k)

main['products name'] = np.where(main['products name'].isnull(),
                                 main['sku id'].map(dict(zip(k['sku id'], k['sku name']))),
                                 main['products name'])    

fab_data_main = main.copy()
print('\nyour df is called "fab_data_main"\n')
print('\n== fab_data end ==\n')    