#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, for lat span, calculate bt avg(obs-sim), 
STDDEV_POP(obs-sim), etc. by each 12 or 6 hours. insert to db.
and, draw bt lat png.

Usage:
    calc_draw_lat_by_point.py --sat=fy3c --ins=mwhs --nwp=t639 --lat_span=5
    --hour_span=6 --just_calc=false --just_draw=false --save_hdf=false
    --date=now

Arguments:
    sat: the satellite you want to calc, fy3c
    ins: the insatrument you want to calc, mwts
    nwp: t639 or ncep
    lat_span: latitude span, 1,2,5,10. ONLY 5 is supported now.
    hour_span: hour span, 12 or 6. ONLY 12 is supported now.
    just_calc: true: only calc lat to db, do NOT draw png.
    just_draw: true: only draw png, do NOT calc lat to db.
    save_hdf: true: after draw png, save hdf in tmp path.
    date: calc for special date. YYYY-mm-dd-h like 2014-04-24-18 [default: now]
        and, 2014-04-24-18 means calc for 00:00-->11:59, and, save result to
        stat table ymdh = 2014-04-24 00:00, means 00:00-->11:59
        
eg:
if we are crontabed at 2013-12-06 04:00, we should calc for previous time zone 
(04:00-12) = -08:00, that is: (yesterday)12:00-->23:59's data, and , save 
result to stat table ymdh = 2013-12-05 12:00, means 12:00-->23:59.

Why? if we just calc for cur time zone, 2013-12-06 00:00 to 03:59, the HDF data 
with time FY3C_MWTSX_GBAL_20131206_0355*** may NOT be arrived for some
acceptable reason. such as redo in COSS, or network reason. 

mysql table like:
    create table *_6H[|12H]_5LAT(ymdh datetime, sim_mod char(5), channel int, 
    lat int, num int, avg float, std float)
"""

__author__ = 'wzq'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
# crontabed every 12 hours, then
#      check ps result, kill previous same program, avoiding hang.
#      get time span, lat span.
#      get calc sql
#      run sql, save calc result to STAT db
#      export all life time series data to hdf
#      draw png
#      mv png to dest path
#         

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
from datetime import datetime
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
hour_span = arguments['--hour_span'].lower()
calc_date = arguments['--date'].lower()
lat_span = arguments['--lat_span'].lower()
just_calc = arguments['--just_calc'].lower()
just_draw = arguments['--just_draw'].lower()
save_hdf = arguments['--save_hdf'].lower()
if 'false' in just_calc:
    just_calc = False
else:
    just_calc = True
if 'false' in just_draw:
    just_draw = False
else:
    just_draw = True
if 'false' in save_hdf:
    save_hdf = False
else:
    save_hdf = True

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

mysql_date_today = calc_date[0:4] + calc_date[5:7] + calc_date[8:10] 
mysql_date_yestday = ''

#get the correct time span.
if calc_date == 'now':
    #calc_date = common.utc_YmdH()
    calc_date1 = datetime.now() + timedelta(days=-3)
    calc_date = calc_date1.strftime('%Y-%m-%d-%H')
    mysql_date_today = calc_date1.strftime('%Y%m%d')
    
timespan = common.get_calc_timespan(calc_date, hour_span)
# timespan = common.get_last_mon_calc_timespan(timespan)
time_tag = timespan['begin_str'] + '`' + timespan['end_str'] + '`'

if timespan['begin_str'][11:13] == '18':
    mysql_date_yestday = timespan['begin_str'][0:4] + timespan['begin_str'][5:7] + timespan['begin_str'][8:10]


# create latitude span list
# factor = int(ins_conf.lat_sds_to_db[1]['factor'])
# lat_list = common.get_data_with_span(-90*factor,90*factor,int(lat_span)*factor)

#stat_6h_lat_point
stat_table = conf.table_setting[sat][ins]['stat_' + hour_span + 'h_' \
                                          + 'lat_point']
insert_sql = 'replace into ' + stat_table + " values('"+timespan['end_str'] \
            + "', %s, %s, %s, %s, %s, %s, %s)"
my_table = []

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, time_tag + msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)
    

def draw_one_channel_crtm(channel):
    tmpfile = conf.tmp_path + '/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d') + '.crtm'

    point_data = numpy.arange(1, ins_conf.pixels + 1, int(1), dtype = numpy.int32)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.db_setting['stat_db']) 
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'channel ' + str(channel) + '`Mysql Fatal Error[' \
            + str(e.args[0])+']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False
    
    sql_time = 'select ymdh from ' + stat_table + " where channel=" \
        + str(channel)+ " group by ymdh"
    begin_sql = 'select ymdh from ' + stat_table + " where channel=" \
                + str(channel) + " and sim_mod=\'crtm\' and point= 1 limit 1" # order by ymdh asc as default.
    end_sql = 'select ymdh from ' + stat_table + " where channel=" \
                + str(channel) + " order by ymdh desc limit 1"
                
    cur.execute(sql_time)
    time_data= cur.fetchall()
    time_cnt = len(time_data)
    cur.execute(begin_sql)
    begin_data = cur.fetchall()
    cur.execute(end_sql)
    end_data = cur.fetchall()

    begin_datetime = begin_data[0][0] + timedelta(hours= 0 - int(hour_span))
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + begin_datetime.strftime("%Y%m%d_%H%M") + '_TO_' \
                + end_data[0][0].strftime("%Y%m%d_%H%M") + '_CH' \
                + format(channel, '02d')+'_'+format(int(hour_span), '02d') \
                + 'H_' + lat_span + 'LAT_' + 'CRTMX' # RTTOV, CRTM                
    tmphdf = tmpfile + '.' + png_title + '.HDF'
    
    
    time_sds = numpy.arange(time_cnt, dtype=int)
    time_sds = map(common.get_time_int, zip(*time_data)[0])    
    num_crtm = numpy.arange(time_cnt*ins_conf.pixels, dtype=int).reshape(ins_conf.pixels ,time_cnt)
    avg_crtm = numpy.arange(time_cnt*ins_conf.pixels, dtype=float).reshape(ins_conf.pixels ,time_cnt)
    std_crtm = numpy.arange(time_cnt*ins_conf.pixels, dtype=float).reshape(ins_conf.pixels,time_cnt)
    rmse_crtm = numpy.arange(time_cnt*ins_conf.pixels, dtype=float).reshape(ins_conf.pixels,time_cnt)
  
    
    for point in xrange(1, ins_conf.pixels+1):
        tmptxt = conf.tmp_path + '/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d') + '_' + str(point) + '.txt'
            
        sql = 'select num, avg, std, rmse from ' + stat_table + " where channel=" \
            + str(channel) + ' and sim_mod=\'crtm\' and point=' + str(point)\
            + conf.export_txt%(tmptxt)
        cur.execute(sql)
        temp_data = numpy.loadtxt(tmptxt,dtype='str',delimiter=',')
        
        num_data = temp_data[: ,0 ]
        avg_data = temp_data[: ,1 ]
        std_data = temp_data[: ,2 ]
        rmse_data = temp_data[: ,3 ]

        if num_data.size != time_cnt:
            print 'data not enought'
            temp_list = []
            temp_list = num_data.tolist()
            tianchong = [-9999]
            temp_list = temp_list + tianchong*(time_cnt - num_data.size )
            num_data = numpy.asarray(temp_list)
            
        if avg_data.size != time_cnt:
            avg_temp_list = []
            avg_temp_list = avg_data.tolist()
            tianchong = [-9999.0]
            avg_temp_list = avg_temp_list + tianchong*(time_cnt - avg_data.size )
            avg_data = numpy.asarray(avg_temp_list)
            
        if std_data.size != time_cnt:
            std_temp_list = []
            std_temp_list = std_data.tolist()
            tianchong = [-9999.0]
            std_temp_list = std_temp_list + tianchong*(time_cnt - std_data.size )
            std_data = numpy.asarray(std_temp_list)

        if rmse_data.size != time_cnt:
            rmse_temp_list = []
            rmse_temp_list = rmse_data.tolist()
            tianchong = [-9999.0]
            rmse_temp_list = rmse_temp_list + tianchong*(time_cnt - rmse_data.size )
            rmse_data = numpy.asarray(rmse_temp_list)
                           
        num_crtm[point-1, : ] = num_data
        avg_crtm[point-1, : ] = avg_data
        std_crtm[point-1, : ] = std_data
        rmse_crtm[point-1, : ] = rmse_data
        
        os.remove(tmptxt)

    cur.close()
    conn.close()
    
    print tmphdf
    hdf = h5.File(tmphdf, 'w') # w: rewrite if hdf already exist. 
    num_crtm = hdf.create_dataset("num_crtm",data=num_crtm.astype(numpy.float32))
    avg_crtm = hdf.create_dataset("avg_crtm",data=avg_crtm.astype(numpy.float32))
    std_crtm = hdf.create_dataset("std_crtm",data=std_crtm.astype(numpy.float32))
    rmse_crtm = hdf.create_dataset("rmse_crtm",data=rmse_crtm.astype(numpy.float32))
    time_crtm = hdf.create_dataset("time_crtm",data=time_sds)
    point = hdf.create_dataset("point",data=point_data)
    hdf.close()
    
    
    tmp_png = tmpfile + '.' + png_title
    cmd = conf.ncl + ' ' + " 'sat=\"" \
        + sat.upper() + "\"' " + " 'instrument=\"" + ins.upper() \
        + "\"' channel=" + str(channel) \
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' "  + " /home/fymonitor/MONITORFY3C/py2/plot/fy3c_mwhs_scan_point_time_crtm.ncl"\
        + ' > ' + tmpfile + '.log' + ' 2>&1'
    print cmd
    (status, output) = commands.getstatusoutput(cmd)
    common.debug(my_log, log_tag, str(status) + '`' + cmd + '`' + output)
    
    dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/' + sat.upper() + '/' + ins.upper() + '/' \
                  + 'LAT_POINT' + '/CRTMX/' 

    arch_path = dest_path + end_data[0][0].strftime("%Y") + '/' \
                    + end_data[0][0].strftime("%m") + '/'
    latest_path = dest_path + 'LATEST/' + format(channel, '02d') + '/'
    print latest_path

    try:
        shutil.copyfile(tmp_png + '.png', arch_path + png_title + '.png')  
        common.empty_folder(latest_path)                        
        common.mv_file(tmp_png + '.png', latest_path + png_title + '.png')
    except (OSError, IOError) as e:
        msg = 'channel ' + format(channel, '02d') + ' png created, but cp ' \
                + 'or mv to dest error[' + str(e.args[0])+']: ' + e.args[1] 
        common.error(my_log, log_tag, time_tag + msg)
        return False

    try:
        os.remove(tmpfile + '.' + png_title + '.png.OK')
        if not save_hdf:
            os.remove(tmphdf)
        os.remove(tmpfile + '.log')
    except OSError,e:
        msg = 'channel ' + format(channel, '02d') +' clean tmp file error[' \
            + str(e.args[0])+']: ' + e.args[1]        
        common.warn(my_log, log_tag, time_tag + msg)
    
    
    return



def draw_one_channel_rttov(channel):
    tmpfile = conf.tmp_path + '/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d')+'.rttov'

    point_data = numpy.arange(1, ins_conf.pixels + 1, int(1), dtype = numpy.int32)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.db_setting['stat_db']) 
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'channel ' + str(channel) + '`Mysql Fatal Error[' \
            + str(e.args[0])+']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False
    
    sql_time = 'select ymdh from ' + stat_table + " where channel=" \
        + str(channel)+ " group by ymdh"
    begin_sql = 'select ymdh from ' + stat_table + " where channel=" \
                + str(channel) + " and sim_mod=\'rttov\' and point= 1 limit 1" # order by ymdh asc as default.
    end_sql = 'select ymdh from ' + stat_table + " where channel=" \
                + str(channel) + " order by ymdh desc limit 1"
                
    cur.execute(sql_time)
    time_data= cur.fetchall()
    time_cnt = len(time_data)
    cur.execute(begin_sql)
    begin_data = cur.fetchall()
    cur.execute(end_sql)
    end_data = cur.fetchall()

    begin_datetime = begin_data[0][0] + timedelta(hours= 0 - int(hour_span))
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + begin_datetime.strftime("%Y%m%d_%H%M") + '_TO_' \
                + end_data[0][0].strftime("%Y%m%d_%H%M") + '_CH' \
                + format(channel, '02d')+'_'+format(int(hour_span), '02d') \
                + 'H_' + lat_span + 'LAT_' + 'RTTOV'# RTTOV, CRTM                
    tmphdf = tmpfile + '.' + png_title + '.HDF'
    
    
    time_sds = numpy.arange(time_cnt, dtype=int)
    time_sds = map(common.get_time_int, zip(*time_data)[0])    
    num_rttov = numpy.arange(time_cnt*ins_conf.pixels, dtype=numpy.float32).reshape(ins_conf.pixels, time_cnt)
    avg_rttov = numpy.arange(time_cnt*ins_conf.pixels, dtype=numpy.float32).reshape(ins_conf.pixels, time_cnt)
    std_rttov = numpy.arange(time_cnt*ins_conf.pixels, dtype=numpy.float32).reshape(ins_conf.pixels, time_cnt)
    rmse_rttov = numpy.arange(time_cnt*ins_conf.pixels, dtype=numpy.float32).reshape(ins_conf.pixels, time_cnt)
  
    
    for point in xrange(1, ins_conf.pixels+1):
        tmptxt = conf.tmp_path + '/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d') + '_' + str(point) + '.txt'
            
        sql = 'select num, avg, std, rmse from ' + stat_table + " where channel=" \
            + str(channel) + ' and sim_mod=\'rttov\' and point=' + str(point)\
            + conf.export_txt%(tmptxt)
        cur.execute(sql)
        temp_data = numpy.loadtxt(tmptxt,dtype='str',delimiter=',')
        
        num_data = temp_data[: ,0 ]
        avg_data = temp_data[: ,1 ]
        std_data = temp_data[: ,2 ]
        rmse_data = temp_data[: ,3 ]

        if num_data.size != time_cnt:
            print 'data not enought'
            temp_list = []
            temp_list = num_data.tolist()
            tianchong = [-9999]
            temp_list = temp_list + tianchong*(time_cnt - num_data.size )
            num_data = numpy.asarray(temp_list)
            
        if avg_data.size != time_cnt:
            avg_temp_list = []
            avg_temp_list = avg_data.tolist()
            tianchong = [-9999.0]
            avg_temp_list = avg_temp_list + tianchong*(time_cnt - avg_data.size )
            avg_data = numpy.asarray(avg_temp_list)
            
        if std_data.size != time_cnt:
            std_temp_list = []
            std_temp_list = std_data.tolist()
            tianchong = [-9999.0]
            std_temp_list = std_temp_list + tianchong*(time_cnt - std_data.size )
            std_data = numpy.asarray(std_temp_list)

        if rmse_data.size != time_cnt:
            rmse_temp_list = []
            rmse_temp_list = rmse_data.tolist()
            tianchong = [-9999.0]
            rmse_temp_list = rmse_temp_list + tianchong*(time_cnt - rmse_data.size )
            rmse_data = numpy.asarray(rmse_temp_list)
                           
        num_rttov[point-1, :] = num_data
        avg_rttov[point-1, :] = avg_data
        std_rttov[point-1, :] = std_data
        rmse_rttov[point-1, :] = rmse_data
        
        os.remove(tmptxt)

    cur.close()
    conn.close()
    
    print tmphdf
    hdf = h5.File(tmphdf, 'w') # w: rewrite if hdf already exist. 
    num_rttov = hdf.create_dataset("num_rttov",data=num_rttov.astype(numpy.float32))
    avg_rttov = hdf.create_dataset("avg_rttov",data=avg_rttov.astype(numpy.float32))
    std_rttov = hdf.create_dataset("std_rttov",data=std_rttov.astype(numpy.float32))
    rmse_rttov = hdf.create_dataset("rmse_rttov",data=rmse_rttov.astype(numpy.float32))
    time_rttov = hdf.create_dataset("time_rttov",data=time_sds)
    point = hdf.create_dataset("point",data=point_data)

    hdf.close()
    

    
    tmp_png = tmpfile + '.' + png_title
    cmd = conf.ncl + ' ' + " 'sat=\"" \
        + sat.upper() + "\"' " + " 'instrument=\"" + ins.upper() \
        + "\"' channel=" + str(channel) \
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' "  + " /home/fymonitor/MONITORFY3C/py2/plot/fy3c_mwhs_scan_point_time_rttov.ncl"\
        + ' > ' + tmpfile + '.log' + ' 2>&1'
    print cmd
    (status, output) = commands.getstatusoutput(cmd)
    common.debug(my_log, log_tag, str(status) + '`' + cmd + '`' + output)
    
    dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/' + sat.upper() + '/' + ins.upper() + '/' \
                  + 'LAT_POINT' + '/RTTOV/' 

    arch_path = dest_path + end_data[0][0].strftime("%Y") + '/' \
                    + end_data[0][0].strftime("%m") + '/'
    latest_path = dest_path + 'LATEST/' + format(channel, '02d') + '/'
    
    print latest_path
    try:
        shutil.copyfile(tmp_png + '.png', arch_path + png_title + '.png')
        common.empty_folder(latest_path)                          
        common.mv_file(tmp_png + '.png', latest_path + png_title + '.png')
    except (OSError, IOError) as e:
        msg = 'channel ' + format(channel, '02d') + ' png created, but cp ' \
                + 'or mv to dest error[' + str(e.args[0])+']: ' + e.args[1] 
        common.error(my_log, log_tag, time_tag + msg)
        return False

    try:
        os.remove(tmpfile + '.' + png_title + '.png.OK')
        if not save_hdf:
            os.remove(tmphdf)
        os.remove(tmpfile + '.log')
    except OSError,e:
        msg = 'channel ' + format(channel, '02d') +' clean tmp file error[' \
            + str(e.args[0])+']: ' + e.args[1]        
        common.warn(my_log, log_tag, time_tag + msg)
        
    
    return
def check_calc_one_channel(channel):
    for i in range(1,5):
        if check_one_channel(channel) != (ins_conf.pixels * 2):
            print 'DATA CHECK ERROR !!!!!!!!!!!!!!'
            calc_one_channel(channel)
            time.sleep(8)
        else:
            print 'DATA CHECK OK !!!!!!!!!!!!!'
            break
    return True
        

def check_one_channel(channel):
    print timespan['end_str']
    sql = "select count(*) from " + stat_table +" where channel=" + str(channel) +" and ymdh like '"\
            + timespan['end_str'] +"';"
    print sql
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.db_setting['stat_db'])
        cur.execute(sql)
        num = cur.fetchall()     
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'channel ' + str(channel) + '`Mysql Fatal Error[' \
            + str(e.args[0])+']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False 

    return num[0][0]



def calc_one_channel(channel):
    rttov_sql = common.get_sql_calc_stat_realtime(channel, ins_conf.sql_rttov_by_point_realtime,
                                         my_table)
    #common.debug(my_log, log_tag, 'channel '+str(channel)+'`rttov`'+rttov_sql)
    #print rttov_sql
    

    crtm_sql = common.get_sql_calc_stat_realtime(channel, ins_conf.sql_crtm_bt_point_realtime,
                                        my_table)
    common.debug(my_log, log_tag, 'channel '+str(channel)+'`crtm`'+crtm_sql)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        point = []
        t= ()
        for point in range(1,ins_conf.pixels+1):       
            conn.select_db(conf.table_setting[sat][ins]['data_db'])
            sql = rttov_sql%((point, timespan['begin_str'], timespan['end_str'],)*len(my_table))

            cur.execute(sql)
            common.debug(my_log, log_tag, sql)
            
            '''
            +----------+----------------------------------+-----------------------------------------+----------------------------------------------------------+
            | count(*) | avg(obs_bt1/100 - rttov_bt1/100) | STDDEV_POP(obs_bt1/100 - rttov_bt1/100) | sqrt(sum(pow((obs_bt1/100 - rttov_bt1/100),2))/count(*)) |
            +----------+----------------------------------+-----------------------------------------+----------------------------------------------------------+
            |     2931 |                       1.82920505 |                              7.61995032 |                                         7.83642992577464 |
            +----------+----------------------------------+-----------------------------------------+----------------------------------------------------------+
            '''
            avg = std = 0
            ret = cur.fetchall()
        
            if ret[0][1] is None:
                avg = 0
            else:
                avg = ret[0][1]
            if ret[0][2] is None:
                std = 0
            else:
                std = ret[0][2]
            if ret[0][3] is None:
                rmse = 0
            else:
                rmse = ret[0][3]
            rttov_data = ('rttov', channel, point, ret[0][0],
                          avg, std, rmse)
            
            sql = crtm_sql%((point,timespan['begin_str'], timespan['end_str'],)*len(my_table)) 
     
            cur.execute(sql)
            avg = std = 0
            ret = cur.fetchall()
            if ret[0][1] is None:
                avg = 0
            else:
                avg = ret[0][1]
            if ret[0][2] is None:
                std = 0
            else:
                std = ret[0][2]
            if ret[0][3] is None:
                rmse = 0
            else:
                rmse = ret[0][3]
            crtm_data = ('crtm', channel, point, ret[0][0],
                          avg, std, rmse)
            
            conn.select_db(conf.db_setting['stat_db'])
            cur.executemany(insert_sql, [rttov_data, crtm_data])
            conn.commit()

        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'channel ' + str(channel) + '`Mysql Fatal Error[' \
            + str(e.args[0])+']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False    
    
    return True
    
def calc_draw_one_channel(channel):
    print channel
    if just_draw:
        print 'just_draw'
        draw_one_channel_crtm(channel)
        draw_one_channel_rttov(channel)
        return 
    
    if just_calc:
        print 'just_calc'
        check_calc_one_channel(channel)
        #calc_one_channel(channel)
        return 

    # calc and draw.
    #if not calc_one_channel(channel):    
    if not check_calc_one_channel(channel):
        print 'calc_one_channel'
        return False
    else:
        print 'draw_one_channel'
        draw_one_channel_crtm(channel)
        draw_one_channel_rttov(channel)
        return 
    
def main():
    global my_table
    
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
        + "' | " + conf.grep + " '\-\-hour_span=" + hour_span \
        + "' | " + conf.grep + " '\-\-lat_span=" + lat_span \
        + "' | " + conf.awk + " '{print $4}'"
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
    if len(mysql_date_yestday) != 0 :
        mysql_yestday = 'show tables like \'%' + mysql_date_yestday + '%T639\''
        try:
            conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                                 user=conf.db_setting['master']['user'],
                                 passwd=conf.db_setting['master']['pwd'], 
                                 port=conf.db_setting['master']['port'])
            cur=conn.cursor()
            conn.select_db(conf.table_setting[sat][ins]['data_db'])
            cur.execute(mysql_yestday) # the result is already sorted by ascii.
            yestday_tables = cur.fetchall()
            cur.close()
            conn.close()
        except MySQLdb.Error, e:
            msg = 'Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
            common.err(my_log, log_tag, time_tag + msg)
            sys.exit(3)

        # ignore L1B table.
        l1b_table = [ x for x in yestday_tables if nwp.upper() in x[0] ]
        for idx, one_table in enumerate(l1b_table):
            my_table.extend([one_table[0]])
   

    mysql_today = 'show tables like \'%' + mysql_date_today + '%T639\''
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(mysql_today) # the result is already sorted by ascii.
        today_tables = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = 'Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, time_tag + msg)
        sys.exit(3)
   
    # ignore L1B table.
    l1b_table = [ x for x in today_tables if nwp.upper() in x[0] ]
    for idx, one_table in enumerate(l1b_table):
        my_table.extend([one_table[0]])

    print my_table

    if len(my_table)<=0:
        print 'FAILED no table found '
        common.err(my_log, log_tag, time_tag + 'FAILED`no table found')
        sys.exit(4)
        
    #for channel in range(1,ins_conf.channels+1):
    #    print channel
    #    calc_draw_one_channel(channel)
    #return

    pool = Pool(2)
    ret = pool.map(calc_draw_one_channel, range(1, ins_conf.channels + 1) )
    pool.close()
    pool.join()
    
    if False in ret:
        msg = 'FAILED`some png may NOT draw.`timeuse='
    else:
        msg = 'SUCC`program finish.`timeuse='
   # msg = 'SUCC`program finish.`timeuse='    
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    common.info(my_log, log_tag, time_tag + msg + timeuse)

if __name__ == '__main__':
    main()

