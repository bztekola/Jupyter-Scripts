import sys
sys.path.insert(0,'/Users/jarad/Scripts/Fake Folder Copy/Python Libraries')
from jb_libraries import *

print('\n== part_categories start ==\n')
cats_master = pd.read_sql(
'''
SELECT
part_id AS 'part id',
master_categories_id AS 'master cat'
FROM parts
WHERE master_categories_id != 0
''', db)
cats_master_mat = cats_master.values

cats_main = pd.read_sql(
'''
SELECT
categories_id AS 'cat id',
parent_id AS 'parent id'
FROM categories
''', db)
cats_main_mat = cats_main.values

cats_names = pd.read_sql(
'''
SELECT
categories_id AS 'cat id',
categories_name AS 'cat name'
FROM categories_description
''', db)

all_cats_dict = {}
for i in range(len(cats_master_mat)):
    inter = []
    inter.append(cats_master_mat[i][0]) # part id
    inter.append(cats_master_mat[i][1]) # master cat id
    try:
        while inter[-1] != 0:
            inter.append(cats_main_mat[cats_main_mat[:,0] == inter[-1]][0][1]) # next cat id leading up to parent id
        all_cats_dict[inter[0]] = inter[-2]
    except:
        pass
    
all_cats_df = pd.DataFrame.from_dict(all_cats_dict, orient = 'index').reset_index()
all_cats_df.columns = ['part id','parent cat']    
all_cats_df['cat name'] = all_cats_df['parent cat'].map(dict(zip(cats_names['cat id'], cats_names['cat name'])))

print('\nyour df is called "all_cats_df"\n')
print('\n== part_categories end ==\n')