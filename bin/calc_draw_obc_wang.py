#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, draw OBC 2-dim and 3-dim data every 12 or 6 hours.

Usage:
    calc_draw_obc.py --sat=fy3c --ins=mwts --span=12 --date=now

Arguments:
    sat: the satellite you want to calc, fy3c
    ins: the insatrument you want to calc, mwts
    span: hour span, 12 or 6. ONLY 12 is supported now.
    date: draw for special date. YYYY-mm-dd-h like 2014-04-24-12 [default: now]
        where 2014-04-24-12 means draw for launch time to today's 11:59

eg:
if we are crontabed at 2013-12-06 14:00, we should draw for previous time zone 
(14:00-12) = 02:00, that is: launch time to today's 11:59's data.
"""

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.
#    
# work flow:
# crontabed every 12 or 6 hours, then
#      check ps result, kill previous same program, avoiding hang.
#      get time span
#      get obc table list
#      export all life obc data to hdf
#      draw png
#      mv png to dest path
#         
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/calc_draw_obc.py 
# --sat=fy3c --ins=mwts --nwp=t639 --date=now
# >> /home/fymonitor/DATA/LOG/calc_draw_obc.py.log 2>&1 &
#                         
# date          author    changes
# 2014-05-19    gumeng    update
# 2014-04-28    gumeng    create

import os
import sys
import time
import numpy
import signal
import commands
import warnings
import MySQLdb
import h5py as h5
import shutil
#import datetime
#from datetime import *
from datetime import datetime
from datetime import timedelta
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 

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
hour_span = arguments['--span'].lower()
orig_calc_date = arguments['--date'].lower()
calc_date = orig_calc_date

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
timespan = common.get_calc_timespan(calc_date, hour_span)
timespan['begin_t'] = 0 # the min time stamp before launch.
time_tag = timespan['begin_str'] + '`' + timespan['end_str'] + '`'

# mysql tables we should draw
my_channel_table = []
my_obc_table = []
my_Calc_table = []

Calc_table_3day = []
Calc_table_1month =[]

obc_table_3day = []
obc_table_1month = []

channel_table_3day = []
channel_table_1month = []

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, time_tag + msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

# create hdf by obc setting
def create_obc_hdf(name_tage,hfile,numpy_data, obc_setting):

    try:
        previous_idx = 0 # time idx = 0
        for one_setting in obc_setting:
            start_idx = previous_idx + 1
            for one_column in xrange(1, one_setting['columns'] + 1):
                if one_setting['columns'] == 1:
                    ds_name = one_setting['db_field'] + '_' + name_tage
                else:
                    ds_name = one_setting['db_field'] + str(one_column) + '_'+ name_tage

                idx = start_idx + one_column - 1
                factor = int(one_setting['factor'])          
                
                if 'float' in one_setting['hdf_dtype'] \
                    and 'int' in one_setting['db_dtype']:
                    hfile.create_dataset(ds_name, data = \
                            numpy_data[:, idx].astype(numpy.float32)*1/factor)
                elif 'tinyint' in one_setting['hdf_dtype'] \
                    and 'tinyint' in one_setting['db_dtype']:
                    hfile.create_dataset(ds_name, data = \
                            numpy_data[:, idx].astype(numpy.float32))
                elif 'int' in one_setting['hdf_dtype'] \
                    and 'int' in one_setting['db_dtype']:
                    hfile.create_dataset(ds_name, data = \
                            numpy_data[:, idx].astype(numpy.int32)*0.001)
                elif 'float' in one_setting['hdf_dtype'] \
                    and 'float' in one_setting['db_dtype']:
                    hfile.create_dataset(ds_name, data = \
                            numpy_data[:, idx].astype(numpy.float32))
                
                else:
                    pass
                
                previous_idx += 1
    except:
        return False
    
    return True

def draw_one_channel(channel):
    if len(channel_table_3day) <= 0:
        return True
    if len(channel_table_1month) <= 0:
        return True
    
    tmpfile3day = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d') + '.3day'
    sql_3day = common.get_obc_3dim_sql(ins_conf.obc_3dim_to_db.values(),str(channel),
                                  channel_table_3day, conf.obc_select_prefix_sql,
                                  conf.obc_3dim_where_sql) \
        + conf.export_txt%(tmpfile3day + '.txt')
    begin_sql_3day = conf.obc_select_prefix_sql + ' 1 from ' + channel_table_3day[0] \
                + conf.obc_3dim_where_sql + str(channel) + " limit 1"
                
    tmpfile1month = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d')+ '.1month' 
    sql_1month = common.get_obc_3dim_sql(ins_conf.obc_3dim_to_db.values(),str(channel),
                                  channel_table_1month, conf.obc_select_prefix_sql,
                                  conf.obc_3dim_where_sql) \
        + conf.export_txt%(tmpfile1month + '.txt')

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])      
        cur.execute(sql_3day)
        cur.execute(sql_1month)
        cur.execute(begin_sql_3day)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 3-dim ch' + str(channel) + ' png`Mysql Fatal Error[' \
            + str(e.args[0]) + ']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False

    tmpfile1year = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() + '.CH' + format(channel, '02d') +'.1year' + '.obc'
    tmpfilelife = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() + '.CH' + format(channel, '02d') +'.life' + '.obc'
    get_channel_txt(tmpfile1year,tmpfilelife,'agc1',channel)
    get_channel_txt(tmpfile1year,tmpfilelife,'agc2',channel)
    get_channel_txt(tmpfile1year,tmpfilelife,'cal_coef1',channel)
    get_channel_txt(tmpfile1year,tmpfilelife,'cal_coef2',channel)
    get_channel_txt(tmpfile1year,tmpfilelife,'cal_coef3',channel)
    get_channel_txt(tmpfile1year,tmpfilelife,'cold_cnt_avg',channel)
    get_channel_txt(tmpfile1year,tmpfilelife,'hot_cnt_avg',channel)
    
    
    tmphdf = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS()+ '.CH' +format(channel, '02d')+'.HDF'
    print tmphdf
    # trans txt result to numpy fmt, to easy hdf create.
    data_3day = numpy.loadtxt(tmpfile3day + '.txt', dtype='str', delimiter=',')
    data_1month = numpy.loadtxt(tmpfile1month + '.txt', dtype='str', delimiter=',')
    

    hfile = h5.File(tmphdf, 'w') # w: rewrite if hdf already exist.
    ymdh_arr = numpy.array(map(common.time_to_arr, data_1month[:, 0]) )
    hfile.create_dataset("time_1month", data = ymdh_arr.astype(numpy.int32)) 
    ret = create_obc_hdf('1month',hfile,data_1month, ins_conf.obc_3dim_to_db.values())
    if not ret:
        return False
    
    ymdh_arr = numpy.array(map(common.time_to_arr, data_3day[:, 0]) )
    hfile.create_dataset("time_3day", data = ymdh_arr.astype(numpy.int32))     
    ret = create_obc_hdf('3day',hfile,data_3day, ins_conf.obc_3dim_to_db.values())
    if not ret:
        return False

    create_1year_hdf_channel(tmpfile1year,tmpfilelife,'agc1',hfile)
    create_1year_hdf_channel(tmpfile1year,tmpfilelife,'agc2',hfile)
    create_1year_hdf_channel(tmpfile1year,tmpfilelife,'cal_coef1',hfile)
    create_1year_hdf_channel(tmpfile1year,tmpfilelife,'cal_coef2',hfile)
    create_1year_hdf_channel(tmpfile1year,tmpfilelife,'cal_coef3',hfile)
    create_1year_hdf_channel(tmpfile1year,tmpfilelife,'cold_cnt_avg',hfile)
    create_1year_hdf_channel(tmpfile1year,tmpfilelife,'hot_cnt_avg',hfile)

    hfile.close()
    


    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_CH01_[PRT|INS_TEMP|...]
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + begin_data[0]['ymdhms'].strftime("%Y%m%d") + '_' \
                + begin_data[0]['ymdhms'].strftime("%H%M") + '_TO_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
                + '_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
                + '_' \
                + format(int(hour_span), '02d') + 'H_CH' \
                + format(channel,'02d')
    
    ret = draw_channel(tmphdf, format(channel,'02d'), png_title,begin_data[0]['ymdhms'].strftime("%Y%m%d"),\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))
    
    return True

def draw_channel(tmphdf, channel, png_title, begin_time, end_time):
    cmd = []
    sds_len = len(conf.draw_ncl_new[ins]['ncl_prog_channel'])
    for i in xrange(0, sds_len): 
        file_out = "/home/fymonitor/MONITORFY3C/py2/plot/" + png_title + '_' + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png']
        ncl_name = "/home/fymonitor/MONITORFY3C/py2/plot/" + sat.upper() + '_' + ins.upper()+'_' + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png'] + '_4SUB.ncl'
        temp_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.' + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png'] +'.log'
        temp_cmd = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
            + "'instrument=\"" + ins.upper() + "\"' channel=" + str(channel) \
            + " 'file_in=\"" + tmphdf + "\"' " \
            + " 'file_out=\"" + file_out + "\"' " \
            + " 'file_title=\"" + png_title + '_' +conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png'] + "\"' " +  ncl_name \
            + ' > ' + temp_log + ' 2>&1'
        print temp_cmd
        
        cmd.append(temp_cmd)

    #print cmd

    #use map:16.6s ; not use map:44.52s
    timeuse_begin = time.time()
    
    for cmd_temp in cmd:
        print cmd_temp
        (status, output) = commands.getstatusoutput(cmd_temp)
        common.debug(my_log, log_tag, str(status) + '`' + cmd_temp + '`' + output)
    
#     pooltest = ThreadPool()
#     ret = pooltest.map(commands.getstatusoutput, cmd )
#     pooltest.close()
#     pooltest.join()

    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    print timeuse
    
    sds_len = len(conf.draw_ncl_new[ins]['ncl_prog_channel'])
    for i in xrange(0, sds_len): 
        file_out = "/home/fymonitor/MONITORFY3C/py2/plot/" + png_title + '_' + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png']
        temp_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.' + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png'] +'.log'
        # check png.OK
        if not common.check_file_exist(file_out + '.png', check_ok = True):
            msg = 'ncl program error: output png file not exist.' + file_out
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False
        dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/NEW' + '/' + sat.upper() + '/' + ins.upper() + '/' \
                  + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png'] + '/' 
        arch_path = dest_path + str(end_time[0:4]) + '/'
        latest_path = dest_path + 'LATEST/' + str(channel) + '/'
        
        try:
            shutil.copyfile(file_out + '.png', arch_path + png_title + '_' + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png'] + '.png')
            common.empty_folder(latest_path)
            common.mv_file(file_out + '.png', latest_path + png_title + '_' + conf.draw_ncl_new[ins]['ncl_prog_channel'][i]['tmp_png'] + '.png')
            os.remove(file_out + '.png.OK')
            os.remove(temp_log)
        except:
            msg = 'png created, but cp or mv to dest error'
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False
    try:
        os.remove(tmphdf)
    except OSError,e:
        msg = 'clean tmp file error[' + str(e.args[0])+']: ' + e.args[1] 
        print msg             
        common.warn(my_log, log_tag, time_tag + msg)

    return True

def get_channel_txt(tmpfile1year,tmpfilelife,type,channel):
    #now_time = datetime.datetime.now()
    now_time = datetime.now()
    #yes_time = now_time + datetime.timedelta(days=(-365))
    yes_time = now_time + timedelta(days=(-365))
    last_year = yes_time.strftime('%Y-%m-%d %H:%M:%S')

    #select * from FY3C_MWTS_DAILY order by ymdh desc limit 365;
    sql_1year = 'select * from FY3C_MWTS_DAILY_13 where type = \''+type + \
                '\'' + ' and channel = ' + str(channel) +' and ymdh > ' + '\'' +last_year + '\'' + \
                conf.export_txt%(tmpfile1year +'.' +type + '.txt')

    sql_life = 'select * from FY3C_MWTS_DAILY_13 where type = \''+type + \
                '\''  + ' and channel = ' + str(channel)\
                + conf.export_txt%(tmpfilelife +'.' +type + '.txt')

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.db_setting['stat_db'])      
        cur.execute(sql_1year)
        cur.execute(sql_life)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'export channel date `Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1] 
        print msg             
        common.err(my_log, log_tag, time_tag + msg)
        return False




#2014-08-08 00:00:00
def get_obc_txt(tmpfile1year,tmpfilelife,type):
    #now_time = datetime.datetime.now()
    now_time = datetime.now()
    #yes_time = now_time + datetime.timedelta(days=(-365))
    yes_time = now_time + timedelta(days=(-365))
    last_year = yes_time.strftime('%Y-%m-%d %H:%M:%S')
    
    
    #select * from FY3C_MWTS_DAILY order by ymdh desc limit 365;
    sql_1year = 'select * from FY3C_MWTS_DAILY1 where type = \''+type + \
                '\'' + ' and ymdh > ' + '\'' +last_year + '\'' + \
                conf.export_txt%(tmpfile1year +'.' +type + '.txt')
    sql_life = 'select * from FY3C_MWTS_DAILY1 where type = \''+type + '\''\
                + conf.export_txt%(tmpfilelife +'.' +type + '.txt')

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.db_setting['stat_db'])      
        cur.execute(sql_1year)
        cur.execute(sql_life)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 2-dim png`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1] 
        print msg             
        common.err(my_log, log_tag, time_tag + msg)
        return False
    
def create_1year_hdf(tmpfile1year,tmpfilelife,type,hfile):
    data_1year = numpy.loadtxt(tmpfile1year +'.' +type + '.txt', dtype='str', delimiter=',')
    ymdh_arr = numpy.array(map(common.time_to_arr, data_1year[:, 0]) )
    hfile.create_dataset(type+"_time_1year", data = ymdh_arr.astype(numpy.int32))
    hfile.create_dataset(type+"_avg_1year",data=data_1year[: ,3].astype(numpy.float32)) 
    hfile.create_dataset(type+"_max_1year",data=data_1year[: ,4].astype(numpy.float32)) 
    hfile.create_dataset(type+"_min_1year",data=data_1year[: ,5].astype(numpy.float32)) 
    hfile.create_dataset(type+"_std_1year",data=data_1year[: ,6].astype(numpy.float32)) 
    
    
    data_life = numpy.loadtxt(tmpfilelife +'.' +type + '.txt', dtype='str', delimiter=',')
    ymdh_arr = numpy.array(map(common.time_to_arr, data_life[:, 0]) )
    hfile.create_dataset(type+"_time_life", data = ymdh_arr.astype(numpy.int32))
    hfile.create_dataset(type+"_avg_life",data=data_life[: ,3].astype(numpy.float32)) 
    hfile.create_dataset(type+"_max_life",data=data_life[: ,4].astype(numpy.float32)) 
    hfile.create_dataset(type+"_min_life",data=data_life[: ,5].astype(numpy.float32)) 
    hfile.create_dataset(type+"_std_life",data=data_life[: ,6].astype(numpy.float32)) 
      
    return True
    

def create_1year_hdf_channel(tmpfile1year,tmpfilelife,type,hfile):
    data_1year = numpy.loadtxt(tmpfile1year +'.' +type + '.txt', dtype='str', delimiter=',')
    ymdh_arr = numpy.array(map(common.time_to_arr, data_1year[:, 0]) )
    hfile.create_dataset(type+"_time_1year", data = ymdh_arr.astype(numpy.int32))
    hfile.create_dataset(type+"_avg_1year",data=data_1year[: ,4].astype(numpy.float32)) 
    hfile.create_dataset(type+"_max_1year",data=data_1year[: ,5].astype(numpy.float32)) 
    hfile.create_dataset(type+"_min_1year",data=data_1year[: ,6].astype(numpy.float32)) 
    hfile.create_dataset(type+"_std_1year",data=data_1year[: ,7].astype(numpy.float32)) 
    
    
    data_life = numpy.loadtxt(tmpfilelife +'.' +type + '.txt', dtype='str', delimiter=',')
    ymdh_arr = numpy.array(map(common.time_to_arr, data_life[:, 0]) )
    hfile.create_dataset(type+"_time_life", data = ymdh_arr.astype(numpy.int32))
    hfile.create_dataset(type+"_avg_life",data=data_life[: ,4].astype(numpy.float32)) 
    hfile.create_dataset(type+"_max_life",data=data_life[: ,5].astype(numpy.float32)) 
    hfile.create_dataset(type+"_min_life",data=data_life[: ,6].astype(numpy.float32)) 
    hfile.create_dataset(type+"_std_life",data=data_life[: ,7].astype(numpy.float32)) 
      
    return True
     

def draw_just_obc():
    if len(obc_table_3day) <= 0:
        return True
    if len(obc_table_1month) <= 0:
        return True
    
    tmpfile3day = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() +'.3day' +'.obc'
    sql_3day = common.get_obc_2dim_sql(ins_conf.obc_to_db.values(),ins_conf.channels,
                                  obc_table_3day, conf.obc_select_prefix_sql) \
        + conf.export_txt%(tmpfile3day + '.txt')
    begin_sql_3day = conf.obc_select_prefix_sql + ' 1 from ' + obc_table_3day[0] \
                + " limit 1"   
    
    tmpfile1month = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() +'.1month' +'.obc'
    sql_1month = common.get_obc_2dim_sql(ins_conf.obc_to_db.values(),ins_conf.channels,
                                  obc_table_1month, conf.obc_select_prefix_sql) \
        + conf.export_txt%(tmpfile1month + '.txt')
        


    tmpfile3day_calc = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() +'.3day.calc' +'.obc'
    sql_3day_calc = common.get_obc_2dim_sql(ins_conf.calc_to_db.values(),ins_conf.channels,
                                  Calc_table_3day, conf.obc_select_prefix_sql) \
        + conf.export_txt%(tmpfile3day_calc + '.txt')  
    
    tmpfile1month_calc = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() +'.1month.calc' +'.obc'
    sql_1month_calc = common.get_obc_2dim_sql(ins_conf.calc_to_db.values(),ins_conf.channels,
                                  Calc_table_1month, conf.obc_select_prefix_sql) \
        + conf.export_txt%(tmpfile1month_calc + '.txt')

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])      
        cur.execute(sql_3day)
        cur.execute(sql_1month)
        cur.execute(sql_3day_calc)
        cur.execute(sql_1month_calc)
        cur.execute(begin_sql_3day)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 2-dim png`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1] 
        print msg             
        common.err(my_log, log_tag, time_tag + msg)
        return False
    

    tmpfile1year = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() +'.1year' + '.obc'
    tmpfilelife = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS() +'.life' + '.obc'
    get_obc_txt(tmpfile1year,tmpfilelife,'cold_ang1')
    get_obc_txt(tmpfile1year,tmpfilelife,'cold_ang2')
    get_obc_txt(tmpfile1year,tmpfilelife,'earth_ang1')
    get_obc_txt(tmpfile1year,tmpfilelife,'earth_ang2')
    get_obc_txt(tmpfile1year,tmpfilelife,'hot_ang1')
    get_obc_txt(tmpfile1year,tmpfilelife,'hot_ang2')
    get_obc_txt(tmpfile1year,tmpfilelife,'ins_temp')
    get_obc_txt(tmpfile1year,tmpfilelife,'prt1')
    get_obc_txt(tmpfile1year,tmpfilelife,'prt2')
    get_obc_txt(tmpfile1year,tmpfilelife,'prt3')
    get_obc_txt(tmpfile1year,tmpfilelife,'prt4')
    get_obc_txt(tmpfile1year,tmpfilelife,'prt5')
    get_obc_txt(tmpfile1year,tmpfilelife,'prt_avg')
    
    get_obc_txt(tmpfile1year,tmpfilelife,'cold_ang_minus')
    get_obc_txt(tmpfile1year,tmpfilelife,'hot_ang_minus')
    get_obc_txt(tmpfile1year,tmpfilelife,'earth_ang_minus')
    get_obc_txt(tmpfile1year,tmpfilelife,'scan_prd')
    

    
    tmphdf = '/assimilation/fymonitor/DATA/TMP/' + my_tag + '.' + common.utc_YmdHMS()+'.OBC.HDF'
    print tmphdf
    # trans txt result to numpy fmt, to easy hdf create.
    data_3day = numpy.loadtxt(tmpfile3day + '.txt', dtype='str', delimiter=',')
    data_1month = numpy.loadtxt(tmpfile1month + '.txt', dtype='str', delimiter=',')
    
    data_3day_calc = numpy.loadtxt(tmpfile3day_calc + '.txt', dtype='str', delimiter=',')
    data_1month_calc = numpy.loadtxt(tmpfile1month_calc + '.txt', dtype='str', delimiter=',')
    print tmpfile1month_calc + '.txt'

    hfile = h5.File(tmphdf, 'w') # w: rewrite if hdf already exist.
    ymdh_arr = numpy.array(map(common.time_to_arr, data_1month[:, 0]) )
    hfile.create_dataset("time_1month", data = ymdh_arr.astype(numpy.int32)) 
    ret = create_obc_hdf('1month',hfile,data_1month, ins_conf.obc_to_db.values())

    ymdh_arr = numpy.array(map(common.time_to_arr, data_3day[:, 0]) )
    hfile.create_dataset("time_3day", data = ymdh_arr.astype(numpy.int32))     
    ret = create_obc_hdf('3day',hfile,data_3day, ins_conf.obc_to_db.values())
    

    ymdh_arr = numpy.array(map(common.time_to_arr, data_1month_calc[:, 0]) )
    hfile.create_dataset("time_1month_calc", data = ymdh_arr.astype(numpy.int32))
    #hfile.create_dataset('cold_ang_minus_1month_calc',data=data_1month_calc[: ,1]*0.001.astype(numpy.int32))
    hfile.create_dataset('cold_ang_minus_1month_calc',data=data_1month_calc[: ,1].astype(numpy.float32)*0.001)
    hfile.create_dataset('hot_ang_minus_1month_calc',data=data_1month_calc[: ,2].astype(numpy.float32)*0.001)
    hfile.create_dataset('earth_ang_minus_1month_calc',data=data_1month_calc[: ,3].astype(numpy.float32)*0.001)
    hfile.create_dataset('scan_prd_1month_calc',data=data_1month_calc[: ,4].astype(numpy.float32))
    #ret = create_obc_hdf('1month_calc',hfile,data_1month, ins_conf.calc_to_db.values())

    ymdh_arr = numpy.array(map(common.time_to_arr, data_3day_calc[:, 0]) )
    hfile.create_dataset("time_3day_calc", data = ymdh_arr.astype(numpy.int32)) 
    hfile.create_dataset('cold_ang_minus_3day_calc',data=data_3day_calc[: ,1].astype(numpy.float32)*0.001)
    hfile.create_dataset('hot_ang_minus_3day_calc',data=data_3day_calc[: ,2].astype(numpy.float32)*0.001)
    hfile.create_dataset('earth_ang_minus_3day_calc',data=data_3day_calc[: ,3].astype(numpy.float32)*0.001)
    hfile.create_dataset('scan_prd_3day_calc',data=data_3day_calc[: ,4].astype(numpy.float32))
        
    #ret = create_obc_hdf('3day_calc',hfile,data_3day, ins_conf.calc_to_db.values())

    
    if not ret:
        return False

    
    create_1year_hdf(tmpfile1year,tmpfilelife,'cold_ang1',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'cold_ang2',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'earth_ang1',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'earth_ang2',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'hot_ang1',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'hot_ang2',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'ins_temp',hfile)
     
    create_1year_hdf(tmpfile1year,tmpfilelife,'prt1',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'prt2',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'prt3',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'prt4',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'prt5',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'prt_avg',hfile)
    
    create_1year_hdf(tmpfile1year,tmpfilelife,'cold_ang_minus',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'hot_ang_minus',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'earth_ang_minus',hfile)
    create_1year_hdf(tmpfile1year,tmpfilelife,'scan_prd',hfile)

    hfile.close()
#    return True

    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_[PRT|INS_TEMP|...]
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + begin_data[0]['ymdhms'].strftime("%Y%m%d") + '_' \
                + begin_data[0]['ymdhms'].strftime("%H%M") + '_TO_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
                + '_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
                + '_' \
                + format(int(hour_span), '02d') + 'H_'

    ret = draw_nochannel(tmphdf, png_title,begin_data[0]['ymdhms'].strftime("%Y%m%d"),\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))

    return True
    
def draw_nochannel(tmphdf, png_title, begin_time, end_time):    
    cmd = []
    sds_len = len(conf.draw_ncl_new[ins]['ncl_prog_no_channel'])
    for i in xrange(0, sds_len): 
        file_out = conf.plot_path + '/'+ png_title + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png']
        file_title = png_title + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png']
        ncl_name = conf.plot_path +'/' + sat.upper() +'_' + ins.upper() +'_'+ conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png'] + '_4SUB.ncl'
        temp_log = conf.tmp_path + '/monitor.' + log_tag +'.' + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png'] +'.log'
        temp_cmd = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
            + "'instrument=\"" + ins.upper() + "\"" +'\'' \
            + " 'file_in=\"" + tmphdf + "\"' " \
            + " 'file_out=\"" + file_out + "\"' " \
            + " 'file_title=\"" + file_title + "\"' " +  ncl_name \
            + ' > ' + temp_log + ' 2>&1'
        print temp_cmd
        cmd.append(temp_cmd)

    #print cmd
 
    timeuse_begin = time.time()
    
    for cmd_temp in cmd:
        print cmd_temp
        (status, output) = commands.getstatusoutput(cmd_temp)
        common.debug(my_log, log_tag, str(status) + '`' + cmd_temp + '`' + output)

#     pooltest = ThreadPool()
#     ret = pooltest.map(commands.getstatusoutput, cmd )
#     pooltest.close()
#     pooltest.join()
    
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    print timeuse

    sds_len = len(conf.draw_ncl_new[ins]['ncl_prog_no_channel'])
    for i in xrange(0, sds_len): 
        file_out = conf.plot_path + '/'+ png_title + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png']
        temp_log = conf.tmp_path + '/monitor.' + log_tag + '.' + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png'] +'.log'
        # check png.OK
        if not common.check_file_exist(file_out + '.png', check_ok = True):
            msg = 'ncl program error: output png file not exist.' + file_out
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False
        
        dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/NEW/'+ sat.upper() + '/' + ins.upper() + '/' \
                  + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png'] + '/' 
        arch_path = dest_path + str(end_time[0:4]) + '/'
        latest_path = dest_path + 'LATEST/' 
        try:
            shutil.copyfile(file_out + '.png', arch_path + png_title + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png'] + '.png')
            common.empty_folder(latest_path)
            common.mv_file(file_out + '.png', latest_path + png_title  + conf.draw_ncl_new[ins]['ncl_prog_no_channel'][i]['tmp_png'] + '.png')
            os.remove(file_out + '.png.OK')
            os.remove(temp_log)
        except:
            msg = 'png created, but cp or mv to dest error'
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False

    try:
        os.remove(tmphdf)
        #os.remove(tmphdf + '.txt')
    except OSError,e:
        msg = 'clean tmp file error[' + str(e.args[0])+']: ' + e.args[1]  
        print msg            
        common.warn(my_log, log_tag, time_tag + msg)
    
    return True


def draw_obc(input):
    #for test!!!!!
#     if input != 'just_obc':
#         return
#     if input != 1:
#         return
    print input
    if input == 'just_obc':
        return draw_just_obc()
    else:
        return draw_one_channel(input)
 
def main():
    global my_channel_table
    global my_obc_table
    
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, time_tag + 'program start')

    # register signal function.
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)      
    
    # check ps result, kill previous same program, avoiding hang.
    # we do NOT grep --date=2014-04-27-18 for convenience.
    cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + conf.bin_path + ' | ' \
        + conf.grep + ' -v grep | ' + conf.grep + ' -v tail | ' + conf.grep \
        + ' -v bash | ' + conf.grep + ' ' + fname + ' | ' + conf.grep \
        + " '\-\-sat=" + sat + "' | " + conf.grep + " '\-\-ins=" + ins \
        + "' | " + conf.grep + " '\-\-span=" + hour_span + "' | " \
        + conf.awk + " '{print $4}'"
    (status, value) = commands.getstatusoutput(cmd)
    pid_list = value.split()
    for one_pid in pid_list:
        if int(one_pid) != pid:
            msg = 'more then one prog find, kill old same prog[' + one_pid + ']'
            common.err(my_log, log_tag, time_tag + msg)
            cmd = conf.kill + ' -kill ' + one_pid
            commands.getstatusoutput(cmd)
    
    #get the correct tables. we MUST get table name from INFO db, not show tables!!
    """
    We MUST create fy3b-mwts table's info, for easy time search
    also, there is a BUG... ...
    """
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute('show tables') # the result is already sorted by ascii.
        all_tables = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = 'Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, time_tag + msg)
        sys.exit(3)
        
    # ignore L1B table.
    all_obc_table = [ x for x in all_tables if 'OBCXX_MS' in x[0] ]
    
    Calc_tag = 'OBCXX_MS_CALC'
    Calc_table = [ x for x in all_obc_table if Calc_tag in x[0]]
    
    channel_tag = 'OBCXX_MS_' + str(ins_conf.channels)
    channel_table = [ x for x in all_obc_table if channel_tag in x[0]]
    
    obc_table = list(set(all_obc_table).difference(set(Calc_table)).difference(set(channel_table))) #return in all_obc_table but no in channel_table
    

    for idx, one_table in enumerate(Calc_table):
        table_t = one_table[0][19:32] # FY3B_MWTSX_GBAL_L1_20131123_0501_060KM_MS
        timeArray = time.strptime(table_t, "%Y%m%d_%H%M")
        cur_timeStamp = time.mktime(timeArray)
        if idx < len(all_tables) - 1:
            table_t = all_tables[idx+1][0][19:32]
            timeArray = time.strptime(table_t, "%Y%m%d_%H%M")
            next_timeStamp = time.mktime(timeArray)
        else:
            timeArray = time.strptime('3013-12-08 00:00', "%Y-%m-%d %H:%M")
            next_timeStamp = time.mktime(timeArray)
        
        if timespan['begin_t'] == cur_timeStamp:
            my_Calc_table.extend([one_table[0]])
        elif timespan['begin_t'] > cur_timeStamp \
            and timespan['begin_t'] < next_timeStamp:
            my_Calc_table.extend([one_table[0]])
        elif timespan['begin_t'] < cur_timeStamp \
            and timespan['end_t'] > cur_timeStamp:
            my_Calc_table.extend([one_table[0]])
        elif timespan['end_t'] == cur_timeStamp:
            break
            
    if len(my_Calc_table)<=0:
        msg = time_tag + 'no table found for my_Calc_table data'
        common.warn(my_log, log_tag, msg)
        
    #now_time = datetime.datetime.now()
    now_time = datetime.now()
    shifen = now_time.strftime("%H%M")
    
    if int(shifen)< 0030:
        for i in range(0, 3):
            yes_time = now_time + datetime.timedelta(days=(-3 + i))
            ymd = yes_time.strftime('%Y%m%d')
            ymd = '201407'+ymd[6:8]
            for idx, one_table in enumerate(my_channel_table):
                if ymd in one_table:
                    Calc_table_3day.extend([one_table])
    else:
        for i in range(0, 3):
            #yes_time = now_time + datetime.timedelta(days=(-2 + i))
            yes_time = now_time + timedelta(days=(-2 + i))
            ymd = yes_time.strftime('%Y%m%d')
            ymd = '201407'+ymd[6:8]
            for idx, one_table in enumerate(my_Calc_table):
                if ymd in one_table:
                    Calc_table_3day.extend([one_table])
   
    for i in range(0, 30):
        #yes_time = now_time + datetime.timedelta(days=(-30 + i))
        yes_time = now_time + timedelta(days=(-30 + i))
        ymd = yes_time.strftime('%Y%m%d')
        ymd = '201407'+ymd[6:8]
        for idx, one_table in enumerate(my_Calc_table):
            if ymd in one_table:
                Calc_table_1month.extend([one_table])

    
#         sys.exit(4)
    
    for idx, one_table in enumerate(obc_table):
        table_t = one_table[0][19:32] # FY3B_MWTSX_GBAL_L1_20131123_0501_060KM_MS
        timeArray = time.strptime(table_t, "%Y%m%d_%H%M")
        cur_timeStamp = time.mktime(timeArray)
        if idx < len(all_tables) - 1:
            table_t = all_tables[idx+1][0][19:32]
            timeArray = time.strptime(table_t, "%Y%m%d_%H%M")
            next_timeStamp = time.mktime(timeArray)
        else:
            timeArray = time.strptime('3013-12-08 00:00', "%Y-%m-%d %H:%M")
            next_timeStamp = time.mktime(timeArray)
        
        if timespan['begin_t'] == cur_timeStamp:
            my_obc_table.extend([one_table[0]])
        elif timespan['begin_t'] > cur_timeStamp \
            and timespan['begin_t'] < next_timeStamp:
            my_obc_table.extend([one_table[0]])
        elif timespan['begin_t'] < cur_timeStamp \
            and timespan['end_t'] > cur_timeStamp:
            my_obc_table.extend([one_table[0]])
        elif timespan['end_t'] == cur_timeStamp:
            break
        
    if len(my_obc_table)<=0:
        msg = time_tag + 'no table found for 2-dims data'
        common.info(my_log, log_tag, msg)
        sys.exit(4)
    
    # sort by filename time asc.
    my_obc_table = sorted(my_obc_table)
    
    #now_time = datetime.datetime.now()
    now_time = datetime.now()
    shifen = now_time.strftime("%H%M")
    
    if int(shifen)< 0030:
        for i in range(0, 3):
            yes_time = now_time + datetime.timedelta(days=(-3 + i))
            ymd = yes_time.strftime('%Y%m%d')
            ymd = '201407'+ymd[6:8]
            for idx, one_table in enumerate(my_obc_table):
                if ymd in one_table:
                    obc_table_3day.extend([one_table])
    else:
        for i in range(0, 3):
            #yes_time = now_time + datetime.timedelta(days=(-2 + i))
            yes_time = now_time + timedelta(days=(-2 + i))
            ymd = yes_time.strftime('%Y%m%d')
            ymd = '201407'+ymd[6:8]
            print ymd
            for idx, one_table in enumerate(my_obc_table):
                if ymd in one_table:
                    obc_table_3day.extend([one_table])
 
    for i in range(0, 30):
        #yes_time = now_time + datetime.timedelta(days=(-30 + i))
        yes_time = now_time + timedelta(days=(-30 + i))
        ymd = yes_time.strftime('%Y%m%d')
        ymd = '201407'+ymd[6:8]
        for idx, one_table in enumerate(my_obc_table):
            if ymd in one_table:
                obc_table_1month.extend([one_table])

### judge channel table
######################################################################
    
    for idx, one_table in enumerate(channel_table):
        table_t = one_table[0][19:32] # FY3B_MWTSX_GBAL_L1_20131123_0501_060KM_MS
        timeArray = time.strptime(table_t, "%Y%m%d_%H%M")
        cur_timeStamp = time.mktime(timeArray)
        if idx < len(all_tables) - 1:
            table_t = all_tables[idx+1][0][19:32]
            timeArray = time.strptime(table_t, "%Y%m%d_%H%M")
            next_timeStamp = time.mktime(timeArray)
        else:
            timeArray = time.strptime('3013-12-08 00:00', "%Y-%m-%d %H:%M")
            next_timeStamp = time.mktime(timeArray)
        
        if timespan['begin_t'] == cur_timeStamp:
            my_channel_table.extend([one_table[0]])
        elif timespan['begin_t'] > cur_timeStamp \
            and timespan['begin_t'] < next_timeStamp:
            my_channel_table.extend([one_table[0]])
        elif timespan['begin_t'] < cur_timeStamp \
            and timespan['end_t'] > cur_timeStamp:
            my_channel_table.extend([one_table[0]])
        elif timespan['end_t'] == cur_timeStamp:
            break
        
    if len(my_obc_table)<=0:
        msg = time_tag + 'no table found for 2-dims data'
        common.info(my_log, log_tag, msg)
        sys.exit(4)
    
    # sort by filename time asc.
    my_channel_table = sorted(my_channel_table)
    
    #now_time = datetime.datetime.now()
    now_time = datetime.now()
    shifen = now_time.strftime("%H%M")

    if int(shifen)< 0030:
        for i in range(0, 3):
            yes_time = now_time + datetime.timedelta(days=(-3 + i))
            ymd = yes_time.strftime('%Y%m%d')
            ymd = '201407'+ymd[6:8]
            for idx, one_table in enumerate(my_channel_table):
                if ymd in one_table:
                    channel_table_3day.extend([one_table])
    else:
        for i in range(0, 3):
            #yes_time = now_time + datetime.timedelta(days=(-2 + i))
            yes_time = now_time + timedelta(days=(-2 + i))
            ymd = yes_time.strftime('%Y%m%d')
            ymd = '201407'+ymd[6:8]
            print ymd
            for idx, one_table in enumerate(my_channel_table):
                if ymd in one_table:
                    channel_table_3day.extend([one_table])
 
    for i in range(0, 30):
        #yes_time = now_time + datetime.timedelta(days=(-30 + i))
        yes_time = now_time + timedelta(days=(-30 + i))
        ymd = yes_time.strftime('%Y%m%d')
        ymd = '201407'+ymd[6:8]
        for idx, one_table in enumerate(my_channel_table):
            if ymd in one_table:
                channel_table_1month.extend([one_table])

######################################################################

#     print channel_table_3day
#     print channel_table_1month
    
    # create input for thread. 'just_obc' means draw 2-dim obc data
    # and, 1...13 means draw 3-dim obc data for each channel
    pool = Pool()
    ret = pool.map(draw_obc, ['just_obc'] + range(1, ins_conf.channels + 1) )
    pool.close()
    pool.join()
    
    if False in ret:
        msg = 'FAILED`some png may NOT draw.`timeuse='
    else:
        msg = 'SUCC`program finish.`timeuse='
        
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    print msg + timeuse
    common.info(my_log, log_tag, time_tag + msg + timeuse)

if __name__ == '__main__':
    main()

