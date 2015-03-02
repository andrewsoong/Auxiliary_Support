#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test program

Usage:
    test.py

"""

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    

import os
import sys
import time
import struct
import random
import signal
import commands
import MySQLdb
import h5py as h5
from multiprocessing import Pool 
from datetime import datetime
from datetime import timedelta


sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

# set read-only global variables.

# main loop
def main():
#     print_sim_bin()
#     test_mkdir()
    test_insert_daily()
#     test_get_files()

def print_sim_bin():
    root = '/assimilation/fymonitor/DATA/MNTOUT/SAT/NSMC/FY3C/MWHS/2014/07/'
    bin_file = root + 'FY3C_MWHSX_GBAL_L1_20140702_0019_015KM_MS_FWDBTS_T639_RTTOV101_TOVSL1X.bin'
    txt_file = bin_file + '.txt'
    
#     sim_fmt = '=' + 'i'*38 + 'd'*2 + 'f'*2 # mwts
    sim_fmt = '=' + 'i'*40 + 'd'*2 + 'f'*2 # mwhs
    
    record = []
    size = struct.calcsize(sim_fmt)
    try:
        file_object = open(bin_file,'rb')
        while True:
            one_record = file_object.read(size)
            if not one_record:
                break
            record.extend([struct.unpack(sim_fmt, one_record)])
    except (OSError, IOError, struct.error) as e:
        msg = 'get_record_from_sim error' + e
        print msg
    finally:
        file_object.close()
    
    txt_output = open(txt_file, 'w+')
    data_cnt = 0
    for one_record in record:
        str_data = ','.join( map(common.data_to_str, one_record) ) + '\n'
        txt_output.writelines(str_data)
        data_cnt = data_cnt + 1
    
    txt_output.close()
    
    print '[%s] %s: read.cnt.from.sim=%s. %s: trans.to.txt.cnt=%s'% \
        (common.utc_nowtime(), bin_file, data_cnt, txt_file, data_cnt)    
    
    
def test_mkdir():
    path = []
    map(create_dir, path)
    
def create_dir(path):
    root = '/assimilation/fymonitor'
    new_path = os.path.join(root, path)
    if not os.path.isdir(new_path):
        os.makedirs(new_path)
        
def get_days():
    begin = datetime.strptime('2012-10-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    end = datetime.strptime('2014-08-08 00:00:00', "%Y-%m-%d %H:%M:%S")
    
    one_day = begin
    days = []
        
    while True:
        if one_day > end:
            break
        else:
            days.append(one_day)
            one_day = one_day + timedelta(days=1)
    
    return days
    
def test_insert_daily():
    sql = "insert into FY3C_MWTS_DAILY values(%s, 'cold_ang1', 0, " \
        + "%s, %s, %s, %s)"
        
    days = map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"), get_days() )
    avg = map(lambda x: 156 + random.uniform(0, 3), xrange(0, len(days)) )
    std = map(lambda x: random.uniform(1, 5), xrange(0, len(days)) )
    max = map(lambda x: 160 + random.uniform(0, 2), xrange(0, len(days)) )
    min = map(lambda x: 140 + random.uniform(0, 3), xrange(0, len(days)) )
    
    data = zip(days, avg, max, min, std)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db('STAT')
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = '`Mysql Fatal Error[' + str(e.args[0])+']: ' + e.args[1]
        print msg     

def test_get_file():
    hdf = common.get_files('/home/fymonitor/MONITORFY3C/py2/bin', '^FY3C_MWTSX_GBAL_L1_\d{8}_\d{4}_\d{3}KM_MS\.HDF\.OK$', '.OK', 'hdf')
    print hdf
        
if __name__ == '__main__':
    main()


