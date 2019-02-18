#=========================
# Libraries
#=========================

import sys
sys.path.insert(0,'/Users/jarad')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xlsxwriter
import datetime as dt
import calendar
import re
import glob
from scipy import stats
import rpy2.rinterface

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('max_colwidth', 100)


#=========================
# My functions
#=========================

def format_(df, fmt):
    # make a copy so u don't alter the origial
    dfcpy = df.copy()
    
    # format headers
    col_list = []
    for col in dfcpy.columns:
        new_col = col.split('_')
        
        new_col02 = []
        caps = ['AOV',
                'aov',
                'ytd',
                'YTD',
                'yoy',
                'cogs',
                'COGS',
                'ups',
                'UPS',
                'dhl',
                'DHL',
                'usps',
                'USPS',
                'oid',
                'OID',
                'Oid']
        for x in col.split(' '):
            if x in caps:
                x = x.upper()
            else:
                x = x.title()
            new_col02.append(x)
            
        new_col02 = ' '.join(new_col02)
        col_list.append(new_col02)
    
    dfcpy.columns = col_list
    
    # format columns
    for ix, f in enumerate(fmt):
        if f == 0:
            pass
        else:
            if f == 'n0':
                fmt = '{:,.0f}'
                mult = 1
            elif f == 'n2':
                fmt = '{:,.2f}'
                mult = 1
            elif f == 'm0':               
                fmt = '${:,.0f}'
                mult = 1
            elif f == 'm2':               
                fmt = '${:,.2f}'
                mult = 1
            elif f == 'p0':
                fmt = '{:,.0f}%'
                mult = 100
            elif f == 'p1':
                fmt = '{:,.1f}%'
                mult = 100
            elif f == 'p2':
                fmt = '{:,.2f}%'
                mult = 100                
            
            dfcpy.iloc[:, ix] = [fmt.format(x * mult) for x in dfcpy.iloc[:, ix]]            
            
    return dfcpy

# add to pandas module
pd.DataFrame.format_ = format_

def col_fix(df):
    df.columns = df.columns.str.replace('_', ' ')

def jb_dates(x, fmt):
    
    x = pd.to_datetime(x)
    x = x.dt.date
    x = x.map(str)

    year = x.str[:4]
    quarter = pd.to_datetime(x).dt.quarter.map(str)
    month_num = x.str[5:7]
    month_name = pd.Series([calendar.month_abbr[x] for x in month_num.map(int)])
    day = x.str[-2:]

    year_and_month = x.str[:7]

    if fmt == 'year and month':
        ret = year_and_month
        
    elif fmt == 'year and month pretty':
        ret = month_name + ' ' + year        

    elif fmt == 'year and month xticks':
        ret = month_name + '\n' + year        
        
#==================================================
        
    elif fmt == 'year and quarter':
        ret = year + '-Q' + quarter        
    
    elif fmt == 'year and quarter pretty':
        ret = 'Q' + quarter + ' ' + year
    
    elif fmt == 'year and quarter xticks':
        ret = 'Q' + quarter + '\n' + year
    
#==================================================    
    
    elif fmt == 'year and week pretty':
        ret = month_name + ' ' + day + ', ' + year
        
    elif fmt == 'year and week xticks':
        ret = month_name + ' ' + day + '\n' + year        

#==================================================
        
    elif fmt == 'year pretty':
        ret = year
        
    elif fmt == 'year xticks':
        ret = year        
        
#==================================================        

    elif fmt == 'date xticks':
        ret = month_name + ' ' + day + '\n' + year
    
    else:
        raise ValueError('incorrect fmt parameter!')
        
    return ret

def jb_mean(df, fmt):
    m = pd.DataFrame(df.mean())
    m.columns = ['mean']
    m = m.T
    m = m.format_(fmt)
    m = m.T
    return m    

def jb_yoy(x):
    ls = ['nan%','inf%','-inf%']
    yoy = x.pct_change(periods = 12)
    yoy = yoy.iloc[[-1]]
    yoy = yoy.format_(['p2'] * len(yoy.columns)).replace(ls, '')
    yoy = yoy.T
    yoy.columns = ['yoy']
    return yoy    

def jb_conf(x):
    m = x.mean()
    s = x.std()
    n = len(x)

    if n < 30:
        alpha = 0.05
        p = stats.t.ppf(1-alpha/2, n-1)
    else:
        p = 1.96

    l = m - p * (s/np.sqrt(n))
    u = m + p * (s/np.sqrt(n))

    a = pd.DataFrame(l)
    b = pd.DataFrame(m)
    c = pd.DataFrame(u)
    d = pd.DataFrame(s)

    cols = ['lower','mean','upper','stand dev']

    df = pd.concat([a,b,c,d], axis = 1)
    df.columns = cols
    df['sample size'] = n
    df = df.T
    
    return df

#=========================
# MySQL Creds
#=========================    

import pymysql

config = {
    'user' : 'reporting',
    'password' : 'fUTFGQoKnyZqVqYFbe#iDd3ppGkJgPoNgnvD',
	'host':'shop-aurora-production-1-us-east-1c.cda0v77r8m9l.us-east-1.rds.amazonaws.com',
    'database' : 'adafruit_zencartnew',
    'port' : '3306'
}

db = pymysql.connect(host=config['host'], port=3306, user=config['user'], passwd=config['password'], db='adafruit_zencartnew', ssl={'ssl':True}, read_timeout = 3600, connect_timeout = 3600)