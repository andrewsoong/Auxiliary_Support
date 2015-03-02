#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, calculate min, max, std, avg of one day obc data.

Usage:
    calc_daily.py --sat=fy3c --ins=iras --nwp=t639 --date=now

Arguments:
    sat: the satellite you want to calc, fy3c
    ins: the insatrument you want to calc, mwts
    nwp: t639 or ncep
    date: calc for special date. YYYY-mm-dd like 2014-04-24 [default: now]

"""

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
# crontabed every day 00:30, then calc, save result to db.
#         
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/calc_daily.py 
# --sat=fy3c --ins=mwts --nwp=t639 --date=now
# >> /home/fymonitor/DATA/LOG/calc_daily.py.log 2>&1
#                         
# date          author    changes
# 2014-08-09    gumeng    create

import os
import sys
import time
import numpy
import shutil
import signal
import commands
import warnings
import MySQLdb
import h5py as h5
from datetime import timedelta
from multiprocessing import Pool 

warnings.filterwarnings('ignore', category = MySQLdb.Warning)

timeuse_begin = time.time()

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

# set read-only global variables.

arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].lower()
ins = arguments['--ins'].lower()
nwp = arguments['--nwp'].lower()
calc_date = arguments['--date'].lower()

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

#get the correct time span.
if calc_date == 'now':
    calc_date = common.utc_YmdH()
else:
    calc_date += '-01' # 2014-08-02-01
timespan = common.get_calc_timespan(calc_date, '24')
# timespan = common.get_last_mon_calc_timespan(timespan)
time_tag = timespan['begin_str'] + '`' + timespan['end_str'] + '`'

# mysql tables we should calc
my_channel_table = []
my_obc_table = []
my_calculate_table=[]

common.wt_file(my_pidfile, str(pid))
common.info(my_log, log_tag, time_tag + 'program start')

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, time_tag + msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

# for each obc setting, calc and insert to STAT.
def calc_just_obc():
    if len(my_obc_table) <= 0:
        return True
    
    
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'], 
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
    except MySQLdb.Error, e:
        msg = 'calc_just_obc`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False
        
    for one_sds in ins_conf.obc_to_db.values():
        for one_column in xrange(1, one_sds['columns'] + 1):
            db_field = one_sds['db_field']
            if one_sds['columns'] is not 1:
                db_field += str(one_column)
            
            calc_sql = common.get_calc_daily_sql(db_field, my_obc_table,
                                                 one_sds['fill_value'],
                                                 conf.calc_daily_prefix_sql,
                                                  conf.calc_daily_subsql,
                                                 conf.calc_daily_postfix_sql)
            common.debug(my_log, log_tag, time_tag + calc_sql)
            try:
                conn.select_db(conf.table_setting[sat][ins]['data_db'])
                cur.execute(calc_sql)
                ret = cur.fetchall() # count(), min(), max(), STDDEV_POP()
                conn.select_db(conf.db_setting['stat_db'])
                data = common.get_data_with_default(ret[0][1:], one_sds['factor'],
                                                    default = 0)
                sql_data = [db_field, ret[0][0] ]
                sql_data.extend(data)
                cur.execute(daily_sql % tuple(sql_data))
                conn.commit()
            except MySQLdb.Error, e:
                msg = 'calc_just_obc`Mysql Fatal Error[' + str(e.args[0]) \
                    + ']: ' + e.args[1]              
                common.warning(my_log, log_tag, time_tag + msg)
    if(ins!='mwri'):   
        for one_sds in ins_conf.calc_to_db.values():
            for one_column in xrange(1, one_sds['columns'] + 1):
                db_field = one_sds['db_field']
                if one_sds['columns'] is not 1:
                    db_field += str(one_column)
            
                calc_sql = common.get_calc_daily_sql(db_field, my_calculate_table,
                                                 one_sds['fill_value'],
                                                 conf.calc_daily_prefix_sql,
                                                  conf.calc_daily_subsql,
                                                 conf.calc_daily_postfix_sql)
                common.debug(my_log, log_tag, time_tag + calc_sql)
                try:
                    conn.select_db(conf.table_setting[sat][ins]['data_db'])
                    cur.execute(calc_sql)
                    ret = cur.fetchall() # count(), min(), max(), STDDEV_POP()
                    conn.select_db(conf.db_setting['stat_db'])
                    data = common.get_data_with_default(ret[0][1:], one_sds['factor'],
                                                    default = 0)
                    sql_data = [db_field, ret[0][0] ]
                    sql_data.extend(data)
                    cur.execute(daily_sql % tuple(sql_data))
                    conn.commit()
                except MySQLdb.Error, e:
                    msg = 'calc_just_obc`Mysql Fatal Error[' + str(e.args[0]) \
                    + ']: ' + e.args[1]              
                    common.warning(my_log, log_tag, time_tag + msg)

    try:
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = 'calc_just_obc`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False

    return True

# for each obc setting, calc and insert to STAT.
def calc_one_channel(channel):
    if len(my_channel_table) <= 0:
        return True
    
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
    except MySQLdb.Error, e:
        msg = 'calc_one_channel ' + str(channel) + '`Mysql Fatal Error[' \
            + str(e.args[0]) + ']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False
        
    for one_sds in ins_conf.obc_3dim_to_calc.values():
        for one_column in xrange(1, one_sds['columns'] + 1):
            db_field = one_sds['db_field']
            if one_sds['columns'] is not 1:
                db_field += str(one_column)
            
            calc_sql = common.get_calc_daily_channel_sql(channel,
                                                 db_field, my_channel_table,
                                                 one_sds['fill_value'],
                                                 conf.calc_daily_prefix_sql,
                                                 conf.calc_daily_channel_subsql,
                                                 conf.calc_daily_postfix_sql)
            common.debug(my_log, log_tag, time_tag + calc_sql)
            try:
                conn.select_db(conf.table_setting[sat][ins]['data_db'])
                cur.execute(calc_sql)
                ret = cur.fetchall() # count(), min(), max(), STDDEV_POP()
                conn.select_db(conf.db_setting['stat_db'])
                data = common.get_data_with_default(ret[0][1:], one_sds['factor'],
                                                    default = 0)
                sql_data = [db_field, channel, ret[0][0] ]
                sql_data.extend(data)
                cur.execute(daily_channel_sql % tuple(sql_data))
                conn.commit()
            except MySQLdb.Error, e:
                msg = 'calc_one_channel ' + str(channel) + '`Mysql Fatal Error[' \
                    + str(e.args[0]) + ']: ' + e.args[1]              
                common.warning(my_log, log_tag, time_tag + msg)

    #try:
    #    cur.close()
    #    conn.close()
    #except MySQLdb.Error, e:
    #    msg = 'calc_one_channel ' + str(channel) + '`Mysql Fatal Error[' \
    #        + str(e.args[0]) + ']: ' + e.args[1]              
    #    common.err(my_log, log_tag, time_tag + msg)
    #    return False
    

    
    calc_daily_prefix_sql = 'select count(value), avg(value), max(value), min(value), ' \
                            'STDDEV_POP(value) from ('
    calc_daily_subsql = 'select value from %s where type= \'%s\' and value!=%s'
    calc_daily_channel_subsql = 'select value from %s where channel=%s and type= \'%s\' and value !=%s'
    calc_daily_postfix_sql = ') as total'

    if(ins=='iras'):
        for one_sds in ins_conf.calc_3dim_to_db.values():
            for one_column in xrange(1, one_sds['columns'] + 1):
                db_field = one_sds['db_field']
                
                if one_sds['columns'] is not 1:
                    db_field += str(one_column)
                calc_sql = calc_daily_channel_sql(channel,
                                                     db_field, my_calc_channel_table,
                                                     one_sds['fill_value'],
                                                     calc_daily_prefix_sql,
                                                     calc_daily_channel_subsql,
                                                     calc_daily_postfix_sql)
                print calc_sql
                common.debug(my_log, log_tag, time_tag + calc_sql)
                try:
                    conn.select_db(conf.table_setting[sat][ins]['data_db'])
                    cur.execute(calc_sql)
                    ret = cur.fetchall() # count(), min(), max(), STDDEV_POP()
                    conn.select_db(conf.db_setting['stat_db'])
                    data = common.get_data_with_default(ret[0][1:], one_sds['factor'],
                                                    default = 0)
                    sql_data = [db_field, channel, ret[0][0] ]
                    sql_data.extend(data)
                    cur.execute(daily_channel_sql % tuple(sql_data))
                    
                    conn.commit()
                except MySQLdb.Error, e:
                    msg = 'calc_one_calc_channel ' + str(channel) + '`Mysql Fatal Error[' \
                        + str(e.args[0]) + ']: ' + e.args[1]              
                    common.warning(my_log, log_tag, time_tag + msg)
            #print'channel table list'
            #print calc_sql

        try:
            cur.close()
            conn.close()
        except MySQLdb.Error, e:
            msg = 'calc_one_calc_channel ' + str(channel) + '`Mysql Fatal Error[' \
                + str(e.args[0]) + ']: ' + e.args[1]  
            print msg            
            common.err(my_log, log_tag, time_tag + msg)
            return False

    return True

def calc_daily_channel_sql(channel, field, tables, fill_value, 
                                prefix_sql, subsql, postfix_sql):

    prefix ='select count(value), avg(value), max(value), min(value), ' \
                            'STDDEV_POP(value) from ('
    subsql_total=''
    for idx, one_table in enumerate(tables):
        if idx == 0:
            subsql_total =  subsql%(one_table,channel,field,fill_value) + subsql_total
        else:
            subsql_total =  subsql%(one_table,channel,field,fill_value) + ' union all ' +subsql_total

    return prefix + subsql_total + postfix_sql


def calc_daily(input):
    print input
    if input == 'just_obc':
        return calc_just_obc()
    else:
        return calc_one_channel(input)

# register signal function.
signal.signal(signal.SIGTERM, signal_handler)   
signal.signal(signal.SIGINT, signal_handler)      

# make sure ONLY ONE prog exist. kill other same program, avoiding hang.
# we do NOT grep --date=2014-04-27-18 for convenience.
cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + conf.bin_path + ' | ' \
	+ conf.grep + ' -v grep | ' + conf.grep + ' -v tail | ' + conf.grep + ' ' \
	+ ' -v bash | ' + conf.grep + ' ' + fname + ' | ' + conf.grep \
	+ " '\-\-sat=" + sat + "' | " + conf.grep + " '\-\-ins=" + ins + "' | " \
	+ conf.grep + " '\-\-nwp=" + nwp + "' | " + conf.awk + " '{print $4}'"
(status, value) = commands.getstatusoutput(cmd)
pid_list = value.split()
for one_pid in pid_list:
	if int(one_pid) != pid:
		msg = 'more then one prog find, kill old same prog[' + one_pid + ']'
		common.err(my_log, log_tag, time_tag + msg)
		cmd = conf.kill + ' -kill ' + one_pid
		commands.getstatusoutput(cmd)

"""
We MUST create fy3b-mwts INFO db, for easy time search, not show tables!!
also, there is a BUG... ...
"""
show_table_sql = "show tables like '%" + timespan['begin_str'][0:4] \
                + timespan['begin_str'][5:7] + timespan['begin_str'][8:10] \
                + "%OBC%' "
try:
    conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                         user=conf.db_setting['master']['user'],
                         passwd=conf.db_setting['master']['pwd'], 
                         port=conf.db_setting['master']['port'])
    cur=conn.cursor()
    conn.select_db(conf.table_setting[sat][ins]['data_db'])
    cur.execute(show_table_sql) # the result is already sorted by ascii.
    all_tables = cur.fetchall()
    cur.close()
    conn.close()
except MySQLdb.Error, e:
	msg = 'FAILED`Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
	common.err(my_log, log_tag, time_tag + msg)
	sys.exit(3)

# ignore L1B table.
#all_obc_table = [ x for x in all_tables if 'OBCXX_MS' in x[0] ]

Calc_tag = 'OBCXX_MS_CALC'
Calc_channel_tag='OBCXX_MS_CALC_'
my_calculate_table = [ x[0] for x in all_tables if Calc_tag in x[0]]
my_calc_channel_table=[ x[0] for x in all_tables if Calc_channel_tag in x[0]]
my_calculate_table =[ x for x in my_calculate_table if Calc_channel_tag not in x]
channel_tag = 'OBCXX_MS_' + str(ins_conf.channels)

my_channel_table = [ x[0] for x in all_tables if channel_tag in x[0]]

my_obc_table = [ x[0] for x in all_tables if 'OBCXX_MS_' not in x[0]]
    #print obc_table


 
print 'begin'
print my_obc_table
print'------------------'
print my_calc_channel_table
print '------------------'
print my_channel_table
print '------------------'
print my_calculate_table

#channel_tag = 'OBCXX_MS_' + str(ins_conf.channels)
#my_channel_table = [ x[0] for x in all_tables if channel_tag in x[0]]
#calculate_tag = 'OBCXX_MS_CALC'
#my_calculate_table=[ x[0] for x in all_tables if calculate_tag in x[0]]
#my_obc_table = [ x[0] for x in all_tables if channel_tag not in x[0]]
#my_obc_table = [ x for x in my_obc_table if calculate_tag not in x]

if len(my_channel_table)<=0:
    msg = time_tag + 'no table found for 3-dims data'
    common.warn(my_log, log_tag, msg)
    sys.exit(4)
if len(my_calculate_table)<=0:
    msg = time_tag + 'no table found for calculate data'
    common.warn(my_log, log_tag, msg)
    
if len(my_calc_channel_table)<=0:
    msg = time_tag + 'no table found for 3-dims calculate data'
    common.warn(my_log, log_tag, msg)
    if(ins=='mwhs'):
        sys.exit(4)
#     if(ins!='mwts'):
#         sys.exit(4)
if len(my_obc_table)<=0:
    msg = time_tag + 'no table found for 2-dims obc data'
    common.error(my_log, log_tag, msg)
    sys.exit(4)
common.debug(my_log, log_tag, "calculate tables:::::::::::::::::::::::::::::::::::::::::::::::::::::::::;")   
print(my_obc_table)
# get insert sql to STAT db.
daily_table = conf.table_setting[sat][ins]['daily']
daily_channel_table = conf.table_setting[sat][ins]['daily_channel']
daily_sql = 'replace into ' + daily_table + " values('"+timespan['begin_str'] \
	    	+ "', '%s', %s, %s, %s, %s, %s)"
daily_channel_sql = 'replace into ' + daily_channel_table + " values('" \
            + timespan['begin_str'] + "', '%s', %s, %s, %s, %s, %s, %s)"

#pool = Pool()
#ret = pool.map(calc_daily, ['just_obc'] + range(1, ins_conf.channels + 1) )
#pool.close()
#pool.join()
calc_daily('just_obc')
for channel in range(1,ins_conf.channels+1):
    calc_daily(channel)


# 
# if False in ret:
# 	msg = 'FAILED`calc failed.`timeuse='
# else:
# 	msg = 'SUCC`program finish.`timeuse='
#  	
# timeuse_end = time.time()
# timeuse = str(round(timeuse_end - timeuse_begin, 2))
# common.info(my_log, log_tag, time_tag + msg + timeuse)

