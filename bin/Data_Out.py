#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, draw OBC 2-dim and 3-dim data every 12 or 6 hours.

Usage:
    calc_draw_4sbus_mwhs.py --sat=fy3c --ins=mwhs --span=12 --date=20140626

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

__author__ = 'hanxl'

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
inputdate = arguments['--date'].lower()
calc_date = inputdate

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
#if calc_date == 'now':
#    calc_date = common.utc_YmdH()
#timespan = common.get_calc_timespan(calc_date, hour_span)
#timespan['begin_t'] = 0 # the min time stamp before launch.
#time_tag = timespan['begin_str'] + '`' + timespan['end_str'] + '`'

# mysql tables we should draw
#my_Calc_channel_table = []
#my_channel_table = []
#my_obc_table = []
#my_Calc_table = []


Calc_table_1month =[]


obc_table_1month = []


channel_table_1month = []


Calc_channel_table_1month = []


T639=[]

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, inputdate + msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

# create hdf by obc setting

def draw_one_channel():
    
    if len(channel_table_1month) <= 0:
        return True
   
    if len(Calc_channel_table_1month) <= 0:
        return True
    
    print"just channel"
      

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])  
        
        
        
        for index,item in enumerate(channel_table_1month):
            
            tmpfile1month = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS() \
            + '.ch' + '.1month' 
            sql_1month = 'select * from '+item[0]+ conf.export_txt%(tmpfile1month + '.txt')
            cur.execute(sql_1month)
            tmphdf = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS()+'.HDF'
            print tmphdf
            data = open(tmpfile1month+'.txt').read() 
            if len(data)==0:
                print "empty table"+item[0]
                continue
            hfile = h5.File(tmphdf, 'w')
            data_1month = numpy.loadtxt(tmpfile1month + '.txt', dtype='str', delimiter=',')
            
            hfile.create_dataset('id', data = (data_1month[:, 0]).astype(numpy.int))
            hfile.create_dataset('channel', data = (data_1month[:, 1]).astype(numpy.int))
            hfile.create_dataset('scln', data = (data_1month[:, 2]).astype(numpy.int))
            hfile.create_dataset('ymdhms', data = (data_1month[:, 3]).astype(numpy.str))
            hfile.create_dataset('cal_coef1', data = (data_1month[:, 4]).astype(numpy.int))
            hfile.create_dataset('cal_coef2', data = (data_1month[:, 5]).astype(numpy.int))
            hfile.create_dataset('cal_coef3', data = (data_1month[:, 6]).astype(numpy.int))
            
            hfile.close()
            
        for index,item in enumerate(Calc_channel_table_1month):
            tmpfile1month_calc = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS() \
            + '.ch' + '.1month_calc'
            sql_1month_calc = 'select * from '+item[0]+ conf.export_txt%(tmpfile1month_calc + '.txt')
            cur.execute(sql_1month_calc)
            tmphdf = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS()+'.HDF'
            print tmphdf
            data = open(tmpfile1month_calc+'.txt').read() 
            if len(data)==0:
                print "empty table"+item[0]
                continue
            hfile = h5.File(tmphdf, 'w')
            data_1month_calc = numpy.loadtxt(tmpfile1month_calc + '.txt', dtype='str', delimiter=',')
            
            hfile.create_dataset('id', data = data_1month_calc[:, 0].astype(numpy.int))
            hfile.create_dataset('channel', data = data_1month_calc[:, 1].astype(numpy.int))
            hfile.create_dataset('scln', data = data_1month_calc[:, 2].astype(numpy.int))
            hfile.create_dataset('ymdhms', data = data_1month_calc[:, 3].astype(numpy.str))
            hfile.create_dataset('gain', data = data_1month_calc[:, 4].astype(numpy.int))
            hfile.create_dataset('agc', data = data_1month_calc[:, 5].astype(numpy.int))
            hfile.create_dataset('SPBB1', data = data_1month_calc[:, 6].astype(numpy.int))
            hfile.create_dataset('SPBB2', data = data_1month_calc[:, 7].astype(numpy.int))
            
            hfile.close()
        
        
        
        
        
            
        
        #cur.execute(sql_1month)
        #print Calc_channel_table_3day
        #print "-------------------------------------"
        #print sql_1month_calc
        
        #cur.execute(sql_1month_calc)
        
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 3-dim ch'  + ' png`Mysql Fatal Error[' \
            + str(e.args[0]) + ']: ' + e.args[1]              
        common.err(my_log, log_tag, inputdate + msg)
        return False

    
    
    #hfile = h5.File(tmphdf, 'w') # w: rewrite if hdf already exist.
    #ymdh_arr = numpy.array(map(common.time_to_arr, data_1month_calc[:, 0]) )
    #hfile.create_dataset("time_1month_channel_calc", data = ymdh_arr.astype(numpy.int32)) 
    #ret = create_obc_hdf('1month',hfile,data_1month_calc, ins_conf.calc_3dim_to_db.values())
    #if not ret:
    #    return False
    
    #ymdh_arr = numpy.array(map(common.time_to_arr, data_3day_calc[:, 0]) )
    #hfile.create_dataset("time_3day_channel_calc", data = ymdh_arr.astype(numpy.int32))     
    #ret = create_obc_hdf('3day',hfile,data_3day_calc, ins_conf.calc_3dim_to_db.values())
    #if not ret:
    #    return False


    
    #return 
    


    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_CH01_[PRT|INS_TEMP|...]
    
    
    return True





#2014-08-08 00:00:00
  
   

     

def draw_just_obc():
    
    if len(obc_table_1month) <= 0:
        return True
    
    if len(Calc_table_1month) <= 0:
        return True
    
    print"just obc"
    #begin_sql_3day = conf.obc_select_prefix_sql + ' 1 from ' + obc_table_3day[0] \
                #+ " limit 1"   
    
    
    #sql_1month = common.get_obc_2dim_sql(ins_conf.obc_to_db.values(),ins_conf.channels,
    #                             obc_table_1month, conf.obc_select_prefix_sql) \
    #  + conf.export_txt%(tmpfile1month + '.txt')
        


    
      
    
    

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])    
        
        for index,item in enumerate(obc_table_1month):
            tmpfile1month = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS() +'.1month' +'.obc'
            sql_1month = 'select * from '+item[0]+ conf.export_txt%(tmpfile1month + '.txt')
            cur.execute(sql_1month)
            tmphdf = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS()+'.OBC.HDF'
            print tmphdf
            data = open(tmpfile1month+'.txt').read() 
            if len(data)==0:
                print "empty table"+item[0]
                continue
            hfile = h5.File(tmphdf, 'w')
            
            data_1month = numpy.loadtxt(tmpfile1month + '.txt', dtype='str', delimiter=',')
            
            hfile.create_dataset('id', data = data_1month[:, 0].astype(numpy.int))
            hfile.create_dataset('scln', data = data_1month[:, 1].astype(numpy.int))
            hfile.create_dataset('ymdhms', data = data_1month[:, 2].astype(numpy.str))
            hfile.create_dataset('ins_temp1', data = data_1month[:, 3].astype(numpy.int))
            hfile.create_dataset('ins_temp2', data = data_1month[:, 4].astype(numpy.int))
            hfile.create_dataset('prt_avg1', data = data_1month[:, 5].astype(numpy.int))
            hfile.create_dataset('prt_avg2', data = data_1month[:, 6].astype(numpy.int))
            hfile.create_dataset('cold_ang', data = data_1month[:, 7].astype(numpy.int))
            hfile.create_dataset('hot_ang', data = data_1month[:, 8].astype(numpy.int))
            hfile.create_dataset('pixviewangle1', data = data_1month[:, 9].astype(numpy.int))
            hfile.create_dataset('pixviewangle2', data = data_1month[:, 10].astype(numpy.int))
            
            hfile.close()
            
        for index,item in enumerate(Calc_table_1month):
            tmpfile1month_calc = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS() +'.1month.calc' +'.obc'
            sql_1month_calc = 'select * from '+item[0]+ conf.export_txt%(tmpfile1month_calc + '.txt')
            cur.execute(sql_1month_calc)
            tmphdf = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS()+'.OBC.HDF'
            print tmphdf
            data = open(tmpfile1month_calc+'.txt').read() 
            if len(data)==0:
                print "empty table"+item[0]
                continue
            hfile = h5.File(tmphdf, 'w')
            data_1month_calc = numpy.loadtxt(tmpfile1month_calc + '.txt', dtype='str', delimiter=',')
            
            hfile.create_dataset('id', data = data_1month_calc[:, 0].astype(numpy.int))
            hfile.create_dataset('scln', data = data_1month_calc[:, 1].astype(numpy.int))
            hfile.create_dataset('ymdhms', data = data_1month_calc[:, 2].astype(numpy.str))
            hfile.create_dataset('digital_control_u', data = data_1month_calc[:, 3].astype(numpy.int))
            hfile.create_dataset('cell_control_u', data = data_1month_calc[:, 4].astype(numpy.int))
            hfile.create_dataset('motor_temp_1', data = data_1month_calc[:, 5].astype(numpy.int))
            hfile.create_dataset('motor_temp_2', data = data_1month_calc[:, 6].astype(numpy.int))
            hfile.create_dataset('antenna_mask_temp_1', data = data_1month_calc[:, 7].astype(numpy.int))
            hfile.create_dataset('antenna_mask_temp_2', data = data_1month_calc[:, 8].astype(numpy.int))
            hfile.create_dataset('fet_118_amp_temp', data = data_1month_calc[:, 9].astype(numpy.int))
            hfile.create_dataset('fet_183_amp_temp', data = data_1month_calc[:, 10].astype(numpy.int))
            hfile.create_dataset('scan_prd', data = data_1month_calc[:, 11].astype(numpy.int))
            
            hfile.close()
        
        
        #cur.execute(begin_sql_3day)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        #print sql_3day
        #print "------------------"
        #print sql_1month
        #print "------------------"
        #print sql_3day_calc
        #print "------------------"
        #print sql_1month_calc
        #print "------------------"
        #print begin_sql_3day
        #print "------------------"
        msg = '??draw obc 2-dim png`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1] 
        print msg             
        common.err(my_log, log_tag, inputdate + msg)
        return False
    
 
   

    
    

#     
#     get_obc_txt(tmpfile1year,tmpfilelife,'cold_ang_minus')
#     get_obc_txt(tmpfile1year,tmpfilelife,'hot_ang_minus')
#     get_obc_txt(tmpfile1year,tmpfilelife,'earth_ang_minus')
#     get_obc_txt(tmpfile1year,tmpfilelife,'scan_prd')
    

    
    
    # trans txt result to numpy fmt, to easy hdf create.
    
    
    
    
    

     # w: rewrite if hdf already exist.
    
    
    #ret = create_obc_hdf('1month',hfile,data_1month, ins_conf.obc_to_db.values())
    

    
    #hfile.create_dataset("time_3day", data = ymdh_arr.astype(numpy.int32)) 

    

    #if not ret:
        #return False

#     ymdh_arr = numpy.array(map(common.time_to_arr, data_1month_calc[:, 0]) )
#     hfile.create_dataset("time_1month_calc", data = ymdh_arr.astype(numpy.int32))
#     #hfile.create_dataset('cold_ang_minus_1month_calc',data=data_1month_calc[: ,1]*0.001.astype(numpy.int32))
#     hfile.create_dataset('cold_ang_minus_1month_calc',data=data_1month_calc[: ,1].astype(numpy.float32)*0.001)
#     hfile.create_dataset('hot_ang_minus_1month_calc',data=data_1month_calc[: ,2].astype(numpy.float32)*0.001)
#     hfile.create_dataset('earth_ang_minus_1month_calc',data=data_1month_calc[: ,3].astype(numpy.float32)*0.001)
#     hfile.create_dataset('scan_prd_1month_calc',data=data_1month_calc[: ,4].astype(numpy.float32))
#     #ret = create_obc_hdf('1month_calc',hfile,data_1month, ins_conf.calc_to_db.values())
# 
#     ymdh_arr = numpy.array(map(common.time_to_arr, data_3day_calc[:, 0]) )
#     hfile.create_dataset("time_3day_calc", data = ymdh_arr.astype(numpy.int32)) 
#     hfile.create_dataset('cold_ang_minus_3day_calc',data=data_3day_calc[: ,1].astype(numpy.float32)*0.001)
#     hfile.create_dataset('hot_ang_minus_3day_calc',data=data_3day_calc[: ,2].astype(numpy.float32)*0.001)
#     hfile.create_dataset('earth_ang_minus_3day_calc',data=data_3day_calc[: ,3].astype(numpy.float32)*0.001)
#     hfile.create_dataset('scan_prd_3day_calc',data=data_3day_calc[: ,4].astype(numpy.float32))
#         
#     #ret = create_obc_hdf('3day_calc',hfile,data_3day, ins_conf.calc_to_db.values())

    
   
    
    
    #sds_len = len(ins_conf.sds_name)
    #for i in xrange(1, sds_len+1):
    #    create_1year_hdf(tmpfile1year,tmpfilelife,ins_conf.sds_name[i]['name'],hfile)
        
#     
#     create_1year_hdf(tmpfile1year,tmpfilelife,'cold_ang_minus',hfile)
#     create_1year_hdf(tmpfile1year,tmpfilelife,'hot_ang_minus',hfile)
#     create_1year_hdf(tmpfile1year,tmpfilelife,'earth_ang_minus',hfile)
#     create_1year_hdf(tmpfile1year,tmpfilelife,'scan_prd',hfile)

    
    #return True

    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_[PRT|INS_TEMP|...]
    

    return True
    
def draw_t639():
    
    if len(T639) <= 0:
        return True
    
    
    
    print"just t639"
    #begin_sql_3day = conf.obc_select_prefix_sql + ' 1 from ' + obc_table_3day[0] \
                #+ " limit 1"   
    
    
    #sql_1month = common.get_obc_2dim_sql(ins_conf.obc_to_db.values(),ins_conf.channels,
    #                             obc_table_1month, conf.obc_select_prefix_sql) \
    #  + conf.export_txt%(tmpfile1month + '.txt')
        


    
      
    
    

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])    
        
        for index,item in enumerate(T639):
            tmpfile1month = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS() +'.1month' +'.obc'
            sql_1month = 'select * from '+item[0]+ conf.export_txt%(tmpfile1month + '.txt')
            cur.execute(sql_1month)
            tmphdf = '/assimilation/fymonitor/DATA/TMP/test/' + item[0] + '.' + common.utc_YmdHMS()+'.HDF'
            print tmphdf
            data = open(tmpfile1month+'.txt').read() 
            if len(data)==0:
                print "empty table"+item[0]
                continue
            hfile = h5.File(tmphdf, 'w')
            print tmpfile1month + '.txt'
            data_1month = numpy.loadtxt(tmpfile1month + '.txt', dtype='str', delimiter=',')
            
            hfile.create_dataset('id', data = data_1month[:, 0].astype(numpy.int))
            hfile.create_dataset('scln', data = data_1month[:, 1].astype(numpy.int))
            hfile.create_dataset('scpt', data = data_1month[:, 2].astype(numpy.int))
            hfile.create_dataset('ymdhms', data = data_1month[:, 3].astype(numpy.str))
            hfile.create_dataset('obt_direct', data = data_1month[:, 4].astype(numpy.str))
            hfile.create_dataset('lat', data = data_1month[:, 5].astype(numpy.int))
            hfile.create_dataset('lon', data = data_1month[:, 6].astype(numpy.int))
            hfile.create_dataset('sen_zen', data = data_1month[:, 7].astype(numpy.int))
            hfile.create_dataset('sen_az', data = data_1month[:, 8].astype(numpy.int))
            hfile.create_dataset('landsea', data = data_1month[:, 9].astype(numpy.int))
            hfile.create_dataset('dem', data = data_1month[:, 10].astype(numpy.int))
            hfile.create_dataset('obs_bt1', data = data_1month[:, 11].astype(numpy.int))
            hfile.create_dataset('obs_bt2', data = data_1month[:, 12].astype(numpy.int))
            hfile.create_dataset('obs_bt3', data = data_1month[:, 13].astype(numpy.int))
            hfile.create_dataset('obs_bt4', data = data_1month[:, 14].astype(numpy.int))
            hfile.create_dataset('obs_bt5', data = data_1month[:, 15].astype(numpy.int))
            hfile.create_dataset('obs_bt6', data = data_1month[:, 16].astype(numpy.int))
            hfile.create_dataset('obs_bt7', data = data_1month[:, 17].astype(numpy.int))
            hfile.create_dataset('obs_bt8', data = data_1month[:, 18].astype(numpy.int))
            hfile.create_dataset('obs_bt9', data = data_1month[:, 19].astype(numpy.int))
            hfile.create_dataset('obs_bt10', data = data_1month[:, 20].astype(numpy.int))
            hfile.create_dataset('obs_bt11', data = data_1month[:, 21].astype(numpy.int))
            hfile.create_dataset('obs_bt12', data = data_1month[:, 22].astype(numpy.int))
            hfile.create_dataset('obs_bt13', data = data_1month[:, 23].astype(numpy.int))
            hfile.create_dataset('obs_bt14', data = data_1month[:, 24].astype(numpy.int))
            hfile.create_dataset('obs_bt15', data = data_1month[:, 25].astype(numpy.int))
            hfile.create_dataset('rttov_bt1', data = data_1month[:, 26].astype(numpy.int))
            hfile.create_dataset('rttov_bt2', data = data_1month[:, 27].astype(numpy.int))
            hfile.create_dataset('rttov_bt3', data = data_1month[:, 28].astype(numpy.int))
            hfile.create_dataset('rttov_bt4', data = data_1month[:, 29].astype(numpy.int))
            hfile.create_dataset('rttov_bt5', data = data_1month[:, 30].astype(numpy.int))
            hfile.create_dataset('rttov_bt6', data = data_1month[:, 31].astype(numpy.int))
            hfile.create_dataset('rttov_bt7', data = data_1month[:, 32].astype(numpy.int))
            hfile.create_dataset('rttov_bt8', data = data_1month[:, 33].astype(numpy.int))
            hfile.create_dataset('rttov_bt9', data = data_1month[:, 34].astype(numpy.int))
            hfile.create_dataset('rttov_bt10', data = data_1month[:, 35].astype(numpy.int))
            hfile.create_dataset('rttov_bt11', data = data_1month[:, 36].astype(numpy.int))
            hfile.create_dataset('rttov_bt12', data = data_1month[:, 37].astype(numpy.int))
            hfile.create_dataset('rttov_bt13', data = data_1month[:, 38].astype(numpy.int))
            hfile.create_dataset('rttov_bt14', data = data_1month[:, 39].astype(numpy.int))
            hfile.create_dataset('rttov_bt15', data = data_1month[:, 40].astype(numpy.int))
            hfile.create_dataset('rttov_nwp_begin_t', data = data_1month[:, 37].astype(numpy.double))
            hfile.create_dataset('rttov_nwp_begin_coef', data = data_1month[:, 38].astype(numpy.float))
            hfile.create_dataset('rttov_nwp_end_t', data = data_1month[:, 39].astype(numpy.double))
            hfile.create_dataset('rttov_nwp_end_coef', data = data_1month[:, 40].astype(numpy.float))
            hfile.create_dataset('crtm_bt1', data = data_1month[:, 26].astype(numpy.int))
            hfile.create_dataset('crtm_bt2', data = data_1month[:, 27].astype(numpy.int))
            hfile.create_dataset('crtm_bt3', data = data_1month[:, 28].astype(numpy.int))
            hfile.create_dataset('crtm_bt4', data = data_1month[:, 29].astype(numpy.int))
            hfile.create_dataset('crtm_bt5', data = data_1month[:, 30].astype(numpy.int))
            hfile.create_dataset('crtm_bt6', data = data_1month[:, 31].astype(numpy.int))
            hfile.create_dataset('crtm_bt7', data = data_1month[:, 32].astype(numpy.int))
            hfile.create_dataset('crtm_bt8', data = data_1month[:, 33].astype(numpy.int))
            hfile.create_dataset('crtm_bt9', data = data_1month[:, 34].astype(numpy.int))
            hfile.create_dataset('crtm_bt10', data = data_1month[:, 35].astype(numpy.int))
            hfile.create_dataset('crtm_bt11', data = data_1month[:, 36].astype(numpy.int))
            hfile.create_dataset('crtm_bt12', data = data_1month[:, 37].astype(numpy.int))
            hfile.create_dataset('crtm_bt13', data = data_1month[:, 38].astype(numpy.int))
            hfile.create_dataset('crtm_bt14', data = data_1month[:, 39].astype(numpy.int))
            hfile.create_dataset('crtm_bt15', data = data_1month[:, 40].astype(numpy.int))
            hfile.create_dataset('crtm_nwp_begin_t', data = data_1month[:, 41].astype(numpy.double))
            hfile.create_dataset('crtm_nwp_begin_coef', data = data_1month[:, 42].astype(numpy.float))
            hfile.create_dataset('crtm_nwp_end_t', data = data_1month[:, 43].astype(numpy.double))
            hfile.create_dataset('crtm_nwp_end_coef', data = data_1month[:, 44].astype(numpy.float))
            
            
            hfile.close()
            
        
        
        
        #cur.execute(begin_sql_3day)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        #print sql_3day
        #print "------------------"
        #print sql_1month
        #print "------------------"
        #print sql_3day_calc
        #print "------------------"
        #print sql_1month_calc
        #print "------------------"
        #print begin_sql_3day
        #print "------------------"
        msg = '??draw obc 2-dim png`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1] 
        print msg             
        common.err(my_log, log_tag, inputdate + msg)
        return False
    
 
   

    
    

#     
#     get_obc_txt(tmpfile1year,tmpfilelife,'cold_ang_minus')
#     get_obc_txt(tmpfile1year,tmpfilelife,'hot_ang_minus')
#     get_obc_txt(tmpfile1year,tmpfilelife,'earth_ang_minus')
#     get_obc_txt(tmpfile1year,tmpfilelife,'scan_prd')
    

    
    
    # trans txt result to numpy fmt, to easy hdf create.
    
    
    
    
    

     # w: rewrite if hdf already exist.
    
    
    #ret = create_obc_hdf('1month',hfile,data_1month, ins_conf.obc_to_db.values())
    

    
    #hfile.create_dataset("time_3day", data = ymdh_arr.astype(numpy.int32)) 

    

    #if not ret:
        #return False

#     ymdh_arr = numpy.array(map(common.time_to_arr, data_1month_calc[:, 0]) )
#     hfile.create_dataset("time_1month_calc", data = ymdh_arr.astype(numpy.int32))
#     #hfile.create_dataset('cold_ang_minus_1month_calc',data=data_1month_calc[: ,1]*0.001.astype(numpy.int32))
#     hfile.create_dataset('cold_ang_minus_1month_calc',data=data_1month_calc[: ,1].astype(numpy.float32)*0.001)
#     hfile.create_dataset('hot_ang_minus_1month_calc',data=data_1month_calc[: ,2].astype(numpy.float32)*0.001)
#     hfile.create_dataset('earth_ang_minus_1month_calc',data=data_1month_calc[: ,3].astype(numpy.float32)*0.001)
#     hfile.create_dataset('scan_prd_1month_calc',data=data_1month_calc[: ,4].astype(numpy.float32))
#     #ret = create_obc_hdf('1month_calc',hfile,data_1month, ins_conf.calc_to_db.values())
# 
#     ymdh_arr = numpy.array(map(common.time_to_arr, data_3day_calc[:, 0]) )
#     hfile.create_dataset("time_3day_calc", data = ymdh_arr.astype(numpy.int32)) 
#     hfile.create_dataset('cold_ang_minus_3day_calc',data=data_3day_calc[: ,1].astype(numpy.float32)*0.001)
#     hfile.create_dataset('hot_ang_minus_3day_calc',data=data_3day_calc[: ,2].astype(numpy.float32)*0.001)
#     hfile.create_dataset('earth_ang_minus_3day_calc',data=data_3day_calc[: ,3].astype(numpy.float32)*0.001)
#     hfile.create_dataset('scan_prd_3day_calc',data=data_3day_calc[: ,4].astype(numpy.float32))
#         
#     #ret = create_obc_hdf('3day_calc',hfile,data_3day, ins_conf.calc_to_db.values())

    
   
    
    
    #sds_len = len(ins_conf.sds_name)
    #for i in xrange(1, sds_len+1):
    #    create_1year_hdf(tmpfile1year,tmpfilelife,ins_conf.sds_name[i]['name'],hfile)
        
#     
#     create_1year_hdf(tmpfile1year,tmpfilelife,'cold_ang_minus',hfile)
#     create_1year_hdf(tmpfile1year,tmpfilelife,'hot_ang_minus',hfile)
#     create_1year_hdf(tmpfile1year,tmpfilelife,'earth_ang_minus',hfile)
#     create_1year_hdf(tmpfile1year,tmpfilelife,'scan_prd',hfile)

    
    #return True

    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_[PRT|INS_TEMP|...]
    

    return True
    
def draw_obc():
    #for test!!!!!
#     if input != 'just_obc':
#         return
#     if input != 1:
#         return
    #print input
    ret=draw_just_obc()
    ret1=draw_one_channel()
    ret2=draw_t639()
    
    #if input == 'just_obc':
    #    return draw_just_obc()
    #else:
    #    return draw_one_channel(input)
    if ret==True and ret1==True and ret2==True:
        return True
    else:
        return False
    
    
 
def main():
    #global my_channel_table
    #global my_Calc_channel_table
    #global my_obc_table
    #global my_Calc_table
    global Calc_table_1month 
    global obc_table_1month
    global channel_table_1month
    global Calc_channel_table_1month
    global T639
    
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, inputdate + 'program start')

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
            common.err(my_log, log_tag, inputdate + msg)
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
        cur.execute("show tables like '%"+inputdate+"%';") # the result is already sorted by ascii.
        #print "show tables like '%"+inputdate+"%'"
        all_tables = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = 'Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, inputdate + msg)
        sys.exit(3)
        
    # ignore L1B table.
    #print all_tables
    all_obc_table = [ x for x in all_tables if 'OBCXX_MS' in x[0] ]
    T639=[ x for x in all_tables if '015KM_MS' in x[0] ]
    Calc_tag = 'OBCXX_MS_CALC'
    Calc_channel_tag='OBCXX_MS_CALC_'
    
    Calc_table = [ x for x in all_obc_table if Calc_tag in x[0]]
    Calc_channel_table=[ x for x in Calc_table if Calc_channel_tag in x[0]]
    
    Calc_channel_table_1month=Calc_channel_table
    
    Calc_table =[ x for x in Calc_table if Calc_channel_tag not in x[0]]
    
    Calc_table_1month=Calc_table
    
    channel_tag = 'OBCXX_MS_' + str(ins_conf.channels)
    channel_table = [ x for x in all_obc_table if channel_tag in x[0]]
    channel_table=list(set(channel_table).difference(set(Calc_table)).difference(set(Calc_channel_table)))
    
    channel_table_1month=channel_table
    
    obc_table = list(set(all_obc_table).difference(set(Calc_table)).difference(set(channel_table)).difference(set(Calc_channel_table))) #return in all_obc_table but no in channel_table
    #print obc_table
    #print "-------------------------"
    #print Calc_table
    #print "-------------------------"
    #print channel_table
    #print "-------------------------"
    #print Calc_channel_table
    obc_table_1month=obc_table
    


    
                
                
                
                
    print "-------------------------"
    print obc_table_1month
    #print "-------------------------"
    #print Calc_table_1month
    #print "-------------------------"
    #print channel_table_1month
    #print "-------------------------"
    #print Calc_channel_table_1month
                
                

######################################################################

    #print obc_table_3day
    #print "##########################################"
    #print my_obc_table
    
    #print channel_table_3day
#     print channel_table_1month
    
    
    #draw_obc('just_obc')
    #for channel in range(1,ins_conf.channels+1):
    #    draw_obc(channel)
    
    # create input for thread. 'just_obc' means draw 2-dim obc data
    # and, 1...13 means draw 3-dim obc data for each channel
    #pool = Pool()
    #ret = pool.map(draw_obc, ['just_obc'] + range(1, ins_conf.channels + 1) )
    #pool.close()
    #pool.join()
    ret= draw_obc() 
    if  ret==False:
        msg = 'FAILED`some png may NOT draw.`timeuse='
    else:
        msg = 'SUCC`program finish.`timeuse='
    
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    print msg + timeuse
    common.info(my_log, log_tag, inputdate + msg + timeuse)

if __name__ == '__main__':
    main()

