#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, draw OBC 2-dim and 3-dim data every 12 or 6 hours.

Usage:
    calc_draw_obc.py --sat=fy3c --ins=mwts --date=201401

Arguments:
    sat: the satellite you want to calc, fy3c
    ins: the insatrument you want to calc, mwts
    date: the month you want to export data
        
"""

__author__ = 'wzq'

# Copyright (c) 2014, shinetek.
# All rights reserved.
#
# /home/fymonitor/python27/bin/python /home/fymonitor/MONITORFY3C/py2/bin/export_obc.py 
# --sat=fy3c --ins=mwts --date=201401
# >> /home/fymonitor/DATA/LOG/calc_draw_obc.py.log 2>&1 &
#                         
# date          author    changes
# 2014-04-28    wzq       create

import os
import re
import sys
import time
import numpy
import signal
import commands
import warnings
import MySQLdb
import string
import h5py as h5
import shutil
from datetime import datetime
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 

warnings.filterwarnings('ignore', category = MySQLdb.Warning)

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')
# set read-only global variables.

arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].lower()
ins = arguments['--ins'].lower()
export_month = arguments['--date']

ins_conf_file = sat.upper() + '_' + ins.upper() + '_CONF'
ins_conf = __import__(ins_conf_file)

if sat not in conf.support_sat_ins or ins not in conf.support_sat_ins[sat]:
    print 'sat or ins setting is NOT found in conf.py'
    sys.exit(0)
    
pid = os.getpid()
fname = os.path.splitext(os.path.basename(os.path.realpath(__file__) ) )[0]
log_tag = fname + '.' + sat + '.' + ins + '.' + str(pid)
my_name = common.get_local_hostname()
my_tag = my_name+'.'+log_tag
my_pidfile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat + '.' \
            + ins + '.pid'
            
my_alivefile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat + '.' \
            + ins + '.alive'
my_log = conf.log_path + '/' + my_name + '.' # just prefix: /log/path/prefix.



# mysql tables we should draw
my_channel_table = []
my_obc_table = []


# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

def export_channel_table(channel):
    if len(my_channel_table) <= 0:
        return True
    tmpfile = conf.txt_path + '/' + sat.upper() + '/' + ins.upper() + '/' + \
            sat.upper() + '_' + ins.upper() + '_' + export_month + '_channel_'\
            + format(int(channel), '02d') + '.txt'
    print tmpfile
    
    sql = common.get_obc_3dim_sql(ins_conf.obc_3dim_to_db.values(),str(channel),
                                  my_channel_table, conf.obc_select_prefix_sql,
                                  conf.obc_3dim_where_sql) \
        + conf.export_txt%(tmpfile)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])      
        cur.execute(sql)
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 3-dim ch' + str(channel) + ' png`Mysql Fatal Error[' \
            + str(e.args[0]) + ']: ' + e.args[1]              
        common.err(my_log, log_tag, msg)
        return False
    return True

def export_obc_table():
    if len(my_obc_table) <= 0:
        return True
    tmpfile = conf.txt_path + '/' + sat.upper() + '/' + ins.upper() + '/' + \
            sat.upper() + '_' + ins.upper() + '_' + export_month + '_obc' + '.txt'
    print tmpfile

    sql = common.get_obc_2dim_sql(ins_conf.obc_to_db.values(),ins_conf.channels,
                                  my_obc_table, conf.obc_select_prefix_sql) \
        + conf.export_txt%(tmpfile)
    
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])      
        cur.execute(sql)
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 2-dim png`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1]              
        common.err(my_log, log_tag,  msg)
        return False

    return True

def export_data(input):
#     if input != 'just_obc':
#         return
    if input == 'just_obc':
        return export_obc_table()
    else:
        return export_channel_table(input)

def combine_channel_txt(channel):
    file_path = conf.txt_path + '/' + sat.upper() + '/' + ins.upper() + '/'
    finally_obc_txt = file_path + sat.upper() + '_' + ins.upper() + \
                    '_LONGLIFE_CHANNEL_' + format(int(channel), '02d') + '.txt'
    all_file = os.listdir(file_path)
    
    file_list = []
    for file in all_file:
        if file=='.' or file=='..' or os.path.isdir(file)==True:
            continue
        if sat.upper() in file:
            if ins.upper() in file:
                if '_channel_' + format(int(channel), '02d') in file:
                    file_list.append(file_path + file) 
                     
    file_sort_by_txt_name(file_list)
    print file_list                
    readsize = 1024*1024        
    output = open(finally_obc_txt, 'w')
    for filename in file_list:
        fileobj  = open(filename, 'r')
        while 1:
            filebytes = fileobj.read(readsize)
            if not filebytes: 
                break
            output.write(filebytes)
        fileobj.close()
    output.close()
    
    
#FY3C_MWTS_201406_obc.txt
def file_sort_by_txt_name(file_list):
    for j in range(len(file_list)-1,-1,-1):
        for i in range(j):
            num1=os.path.basename(file_list[i])[10:16]
            num2=os.path.basename(file_list[i+1])[10:16]
            if int(num1) > int(num2):
                file_list[i],file_list[i+1] = file_list[i+1] ,file_list[i]
    return

    
def combine_obc_txt():
    file_path = conf.txt_path + '/' + sat.upper() + '/' + ins.upper() + '/'
    finally_obc_txt = file_path + sat.upper() + '_' + ins.upper() + '_LONGLIFE_OBC' + '.txt'
    
    all_file = os.listdir(conf.txt_path + '/' + sat.upper() + '/' + ins.upper())
    #print all_file
    
    file_list = []
    for file in all_file:
        if file=='.' or file=='..' or os.path.isdir(file)==True:
            continue
        if sat.upper() in file:
            if ins.upper() in file:
                if '_obc' in file:
                    file_list.append(file_path + file)
                    
    file_sort_by_txt_name(file_list)  
    print file_list              
    readsize = 1024*1024        
    output = open(finally_obc_txt, 'w')
    for filename in file_list:
        fileobj  = open(filename, 'r')
        while 1:
            filebytes = fileobj.read(readsize)
            if not filebytes: 
                break
            output.write(filebytes)
        fileobj.close()
    output.close()

    
    
    
def main():
    global my_channel_table
    global my_obc_table
    
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, 'program start')

    # register signal function.
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)   
    
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        sql_cmd = 'show tables like ' + '\"' + sat.upper() + '_' + ins.upper()\
                + 'X_GBAL_L1_' + export_month +'%\" '
        print sql_cmd
        cur.execute(sql_cmd)
        #cur.execute('show tables like "FY3C_MWTSX_GBAL_L1_201401%" ')  
        all_tables = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = 'Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        sys.exit(3)

    #print all_tables
    # ignore L1B table.
    all_obc_table = [ x for x in all_tables if 'OBCXX' in x[0] ]
    channel_tag = 'OBCXX_MS_' + str(ins_conf.channels)
    channel_table = [ x for x in all_obc_table if channel_tag in x[0]]
    obc_table = list(set(all_obc_table).difference(set(channel_table))) #return in all_obc_table but no in channel_table
    
    for idx, one_table in enumerate(channel_table):
        my_channel_table.extend([one_table[0]])
    
    for idx, one_table in enumerate(obc_table):
        my_obc_table.extend([one_table[0]])
          
    my_obc_table = sorted(my_obc_table)
    my_channel_table = sorted(my_channel_table)
#     print my_obc_table
#     print 'kkkkkkkkkkkkkkkkkkkk'
#     print my_channel_table

    pool = Pool()
    ret = pool.map(export_data, ['just_obc'] + range(1, ins_conf.channels + 1) )
    pool.close()
    pool.join()

    combine_obc_txt()
    for channel in range(1,ins_conf.channels+1):
        combine_channel_txt(channel)



if __name__ == '__main__':
    main()