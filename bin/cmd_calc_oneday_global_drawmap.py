#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:
    calc_oneday_global_drawmap.py --sat=mysat --ins=myins --just_calc=false --date=mydata --noon_flag=myflag

Arguments:
    sat   the satellite name.  like fy3b , fy3c
    ins   the Instrument name. like mwts , mwhs ...
"""

__author__ = 'gumeng'

"""Temp program.

Calc oneday global ,create hdf file And draw maps
Calculate obs-sim, etc. for one day global.
Do NOT insert result to mysql.

eg:
if we are crontabed at 2013-12-06 12:00, we should calc for previous time zone: 
[2013-12-05 00:00, 2013-12-05 23:59]

cron time:        12 [every 24 hours]
---------------------------------------------------------------
calc for:  [yesterday]00:00---23:59
"""

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#                            
# date         author    changes
# 2014-03-28   wangzq    change to V2
#
# 2014-01-24   gumeng    create.

from datetime import timedelta
import numpy
import h5py as h5
import commands
import MySQLdb
import signal
import time
import sys
import os
import threading
import datetime
import shutil
import warnings
from multiprocessing import Pool 
from multiprocessing.dummy import Pool as ThreadPool 

#warnings.filterwarnings('ignore', category = MySQLdb.Warning)

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].upper()
ins = arguments['--ins'].upper()
just_calc = arguments['--just_calc'].lower()
if 'false' in just_calc:
    just_calc = False
else:
    just_calc = True

# systime = arguments['--date']
# noon_flag = arguments['--flag']

systime= datetime.datetime.now() + datetime.timedelta(days=-3)
systime = systime.strftime('%Y%m%d%H%M') 
#print systime
#systime='20140831021221'
sysH = systime[8:10]
if int(sysH) < 12:  
    noon_flag = 'PM'
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-4)
    ymd = yes_time.strftime('%Y-%m-%d')
else:
    noon_flag = 'AM'
    ymd = systime[0:4] + '-' + systime[4:6] + '-' + systime[6:8]
  
# ymd = ymd[0:4] + '-' + '05' + '-' + ymd[8:10]
#ymd = '2014-08-02'
#noon_flag = 'PM'
ymd = arguments['--date']
noon_flag = arguments['--noon_flag']
print ymd
print noon_flag
#sys.exit(0)
timeArray = time.strptime(ymd, "%Y-%m-%d")
ymd = time.strftime("%Y%m%d", timeArray)

timespan = common.get_calc_timespan(common.utc_YmdH(), '12')
time_tag = timespan['begin_str'] + '`' + timespan['end_str'] + '`'


ts_scope = {
        'AM': {'min': ' 00:00:00', 'max': ' 11:59:59'},
        'PM': {'min': ' 12:00:00', 'max': ' 23:59:59'},
}

my_ts = {'min':0, 'max':0}
timeArray = time.strptime(ymd + ts_scope[noon_flag]['min'], "%Y%m%d %H:%M:%S")
my_ts['min'] = time.mktime(timeArray)
timeArray = time.strptime(ymd + ts_scope[noon_flag]['max'], "%Y%m%d %H:%M:%S")
my_ts['max'] = time.mktime(timeArray)


ins_conf_file = sat + '_' + ins + '_CONF'
ins_conf = __import__(ins_conf_file)

channels = ins_conf.channels
pixels = ins_conf.pixels

pid = os.getpid()


# log_tag = sys.argv[0]
# log_tag = log_tag[:-3]
# log_tag = os.path.basename(log_tag)
fname = os.path.splitext(os.path.basename(os.path.realpath(__file__) ) )[0]
log_tag = fname + '.' + sat + '.' + ins + '.' + str(pid)

my_name = common.get_local_hostname()
my_tag = my_name+'.'+log_tag
my_pidfile = conf.pid_path + '/' + my_tag + '.pid'

my_log = conf.log_path + '/' + my_name + '.' # just log prefix: hp-mgmt-1.

def str_to_int(data):  
    strtime = (int(data[0:4]), int(data[5:7]), int(data[8:10]), \
               int(data[11:13]),int(data[14:16]), int(data[17:19]),0,0,0)  
    temp_time= time.mktime(strtime)
#     a= str(data[0:4]) + '-' + str(data[5:7]) + '-' + str(data[8:10]) + ' ' \
#        + str(data[11:13]) +':' + str(data[14:16]) + ':' + str(data[17:19])
#     temp_time= time.mktime(time.strptime(a,'%Y-%m-%d %H:%M:%S'))
#     
    return temp_time
    
    
# Deal with signal.
def signal_handler(signum, frame):
    msg = 'recv signal ' + str(signum) + '. exit now. pid=' + str(pid)
    common.info(my_log, log_tag, time_tag + msg)
    cmd = conf.rm + ' -rf ' + my_pidfile + '2>/dev/null'
    os.system(cmd)
    sys.exit(0)
   
 
#Read from DB and create hdf
def cal_and_create_hdf(channel):
#     if channel!=7:
#         return True
    # get scans count.
    scans_cnt = []
    for one_table in my_table:
        try:
            conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                                 user=conf.db_setting['master']['user'],
                                 passwd=conf.db_setting['master']['pwd'], 
                                 port=conf.db_setting['master']['port'])
            cur=conn.cursor()
            #conn.select_db('FY3C_MWTS')
            conn.select_db(conf.table_setting[sat.lower()][ins.lower()]['data_db'])
            sql = 'select count(id) from ' + one_table
            cur.execute(sql) # the result is already sorted by ascii.
            cnt = cur.fetchone()
            #scans_cnt.append(cnt[0]/2/pixels)
            scans_cnt.append(cnt[0]/pixels)   
        except MySQLdb.Error, e:
            msg = "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
            common.err(my_log, log_tag, time_tag + msg)
            return False
    scans = sum(scans_cnt) #len(rttov_data)/pixels # pixels=15
    print scans

    msg=str(channel) + ': scans=%s'%(scans)
    common.info(my_log, log_tag, time_tag + msg)
    
    # RTTOV Mode
    sql_rttov_temp = 'select ymdhms, lat, lon, obs_bt' + str(channel) + ', ' + \
        'rttov_bt' + str(channel) + ', ' + \
        'obs_bt' + str(channel) + '-rttov_bt' + str(channel) + ' ' + \
        ' from ( ' 
    sub_rttov_select = 'select ymdhms, lat, lon, obs_bt' + str(channel) + ', rttov_bt' \
                + str(channel) + ' '
    total = ' ) as total'
#     sql_rttov = sql_rttov_temp + ' union all '.join(map(lambda x,y: x + ' from ' + y, \
#                 [sub_rttov_select]*len(my_table), my_table) ) + total
    sql_rttov = sql_rttov_temp +' union all '.join(map(lambda x,y: x + ' from ' + y, \
                [sub_rttov_select]*len(my_table), my_table) ) + total 
   
                #
#     rttov_filename = conf.temp_global + sat + '.' + ins + '.' +str(channel) + '.' \
#                     + 'rttov.' + common.utc_YmdHMS() + '.txt'
    rttov_filename = '/home/fymonitor/DATA/TEMPHDF/GLOBAL/' + sat + '.' + ins + '.' +str(channel) + '.' \
                    + 'rttov.' + common.utc_YmdHMS() + '.txt'
    sql_rttov = sql_rttov +' INTO OUTFILE ' + '\'' + rttov_filename + '\'' + \
                ' FIELDS TERMINATED BY ' + '\'' + ','  + '\''   
    
    # CRTM Mode
    sql_crtm_temp = 'select ymdhms, lat, lon, obs_bt' + str(channel) + ', ' + \
        'crtm_bt' + str(channel) + ', ' + \
        'obs_bt' + str(channel) + '-crtm_bt' + str(channel) + ' ' + \
        ' from ( ' 
    sub_crtm_select = 'select ymdhms, lat, lon, obs_bt' + str(channel) + ', crtm_bt' \
                + str(channel) + ' '
    total = ' ) as total'
    sql_crtm = sql_crtm_temp + ' union all '.join(map(lambda x,y: x + ' from ' + y, \
                [sub_crtm_select]*len(my_table), my_table) ) + total
#     crtm_filename = conf.temp_global + sat + '.' + ins + '.' +str(channel) + '.' \
#                     + 'crtm.' + common.utc_YmdHMS() + '.txt'
    crtm_filename = '/home/fymonitor/DATA/TEMPHDF/GLOBAL/' + sat + '.' + ins + '.' +str(channel) + '.' \
                    + 'crtm.' + common.utc_YmdHMS() + '.txt'
    sql_crtm = sql_crtm +' INTO OUTFILE ' + '\'' + crtm_filename + '\'' + \
                ' FIELDS TERMINATED BY ' + '\'' + ','  + '\''   
           
    common.debug(my_log, log_tag,time_tag + sql_rttov)
    common.debug(my_log, log_tag,time_tag + sql_crtm)

    try:
        #conn.select_db('FY3C_MWTS')
        conn.select_db(conf.table_setting[sat.lower()][ins.lower()]['data_db'])  
        cur.execute(sql_rttov)
        cur.execute(sql_crtm)   
    except MySQLdb.Error, e:
        msg = "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
        common.err(my_log, log_tag, time_tag + msg)
        return False
      
    cur.close()
    conn.close()
          
    rttov_arr=list()
    crtm_arr=list()
    rttov_arr = numpy.loadtxt(rttov_filename,dtype='str',delimiter=',')
    crtm_arr  = numpy.loadtxt(crtm_filename,dtype='str',delimiter=',')
    
    common.debug(my_log, log_tag, time_tag +'load file OK!!!')
   
#     filename = conf.temp_global + sat + '_' + ins + '_GLOBAL_' \
#             + ymd + '_CH' + '%02d'%(channel) + '_' + noon_flag + '.HDF'
    filename = '/home/fymonitor/DATA/TEMPHDF/GLOBAL/' + sat + '_' + ins + '_GLOBAL_' \
            + ymd + '_CH' + '%02d'%(channel) + '_' + noon_flag + '.HDF'
                    
    msg =  str(channel) + ' : ' + filename
    common.debug(my_log, log_tag, time_tag + msg)

    
    hdf = h5.File(filename, 'w')
    time_arr=map(str_to_int,rttov_arr[: ,0])
    time_num=numpy.array(time_arr)
    obs_time = hdf.create_dataset("time",data=time_num.astype(numpy.int) \
                        .reshape(scans, pixels)) 
    lat = hdf.create_dataset("lat",data=rttov_arr[: ,1].astype(numpy.float32)\
                        .reshape(scans, pixels)*ins_conf.global_hdf_factor['lat'])
    lon = hdf.create_dataset("lon",data=rttov_arr[: ,2].astype(numpy.float32)\
                        .reshape(scans, pixels)*ins_conf.global_hdf_factor['lon'])
    obs_bt = hdf.create_dataset("obs_bt",data=rttov_arr[: ,3] \
                        .astype(numpy.float32).reshape(scans, pixels)*ins_conf.global_hdf_factor['obs_bt'])
    sim_bt_rttov = hdf.create_dataset("sim_bt_rttov", data=rttov_arr[: ,4]\
                        .astype(numpy.float32).reshape(scans, pixels)*ins_conf.global_hdf_factor['sim_bt_rttov'])
    diff_value_rttov = hdf.create_dataset("diff_rttov", data=rttov_arr[: ,5]\
                        .astype(numpy.float32).reshape(scans, pixels)*ins_conf.global_hdf_factor['diff_rttov'])
    sim_bt_crtm = hdf.create_dataset("sim_bt_crtm", data=crtm_arr[: ,4]\
                        .astype(numpy.float32).reshape(scans, pixels)*ins_conf.global_hdf_factor['sim_bt_crtm'])
    diff_value_crtm = hdf.create_dataset("diff_crtm", data=crtm_arr[: ,5]\
                        .astype(numpy.float32).reshape(scans, pixels)*ins_conf.global_hdf_factor['diff_crtm'])
    scans = numpy.array(scans_cnt)
    obs_scans = hdf.create_dataset("scans",data=scans.astype(numpy.int32)) 
    hdf.close()
    
    msg = str(channel) + ' : hdf file write ok !!!'
    common.info(my_log, log_tag, time_tag + msg)
    msg = str(channel) + ' : ' + common.utc_nowtime()
    common.debug(my_log, log_tag, time_tag + msg)
    
    try:
        os.remove(rttov_filename)
        os.remove(crtm_filename)
        msg = 'remove ' + rttov_filename + ' and ' + crtm_filename + ' Sucess!!' 
        common.info(my_log, log_tag, time_tag + msg)
    except OSError,e: 
        msg = 'remove ' + rttov_filename + ' and ' + crtm_filename + ' Failure!!' 
        common.err(my_log, log_tag, time_tag + msg)
        return False

      
    
#def drawglobalmap(sat,ins,ymd,channel):
def drawglobalmap(channel):
#     if channel!=7:
#         return True
    
    map_ncl = conf.plot_path + '/global_map_new.ncl'
    #map_ncl = conf.plot_path + '/global_map_histogram.ncl'
    temp_ncl_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + '%02d'%(channel)+'.log'
    
#     hdf_name = conf.temp_global + sat + '_' + ins + '_GLOBAL_' + ymd + '_CH' + \
#             '%02d'%(channel) + '_' + noon_flag +'.HDF'
    hdf_name = '/home/fymonitor/DATA/TEMPHDF/GLOBAL/' + sat + '_' + ins + '_GLOBAL_' + ymd + '_CH' + \
            '%02d'%(channel) + '_' + noon_flag +'.HDF'
    filetitle = sat + '_' + ins + '_GLOBAL_' + ymd + '_CH' + \
                '%02d'%(channel) + '_' + noon_flag 
    map_out_name = conf.plot_path + '/' + sat + '_' + ins + \
                '_GLOBAL_' + ymd + '_CH' + '%02d'%(channel)+ '_' + noon_flag
    
    cmd = conf.ncl +' \'' + 'sat=' +'\"' + sat  + '\"\' ' + '\'' + 'instrument=' \
            + '\"' + ins + '\"\' ' + 'channel=' + '%02d'%(channel) + ' \'' + \
            'file_in=' + '\"' + hdf_name + '\"\' ' + '\'' + 'file_out=' + \
            '\"'  + map_out_name + '\"\' ' + " 'file_title=\"" +\
            filetitle + '\"' +'\' ' + map_ncl +' > ' + temp_ncl_log + ' 2>&1'
    print cmd
#> /home/fymonitor/DATA/TMP/monitor.calc_draw_bt.fy3c.mwts.7446.20140526123005.ch05.log 2>&1
    #cmd = 'cd /home/fymonitor/MONITORFY3C/py2/plot ; ' + cmd 
    #cmd = 'cd ' + conf.plot_path + ' ; ' +cmd
    os.system( cmd )
    common.debug(my_log, log_tag, time_tag + cmd)
    
    # check png.OK
    if not common.check_file_exist(map_out_name + '.png', check_ok = True):
        msg = 'ncl program error: output png file not exist.'
        common.error(my_log, log_tag, time_tag + msg)
        return False
    common.debug(my_log, log_tag, time_tag + 'check png OK , all picture has done!!')

    # mv png to img path.
    dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/' + sat.upper() + '/' + ins.upper() + '/' \
              + conf.png_type['global'] + '/' 

    arch_path = dest_path + ymd[0:4] + '/' + ymd[4:6] + '/'
    latest_path = dest_path + 'LATEST/' +  '%02d'%(channel) + '/'
    print arch_path
    #/home/fymonitor/DATA/IMG/NSMC/FY3C/MWTS/GLOBAL/LATEST
 
    try:       
        shutil.copy(map_out_name + '.png', arch_path + filetitle + '.png')
        common.empty_folder(latest_path)
        common.mv_file(map_out_name + '.png', latest_path + filetitle + '.png')
        msg = 'copy and move file Sucess: ' + map_out_name + '.png'
        common.info(my_log, log_tag, time_tag + msg)  
    except:
        msg = 'png created, but cp or mv to dest error'
        common.error(my_log, log_tag, time_tag + msg)
        return False
  
    try:
        os.remove(temp_ncl_log)
        os.remove(map_out_name + '.png.OK')
        os.remove(hdf_name)
        msg = 'Move hdf file and Picture.Ok and ncl temp log  Sucess: ' + \
                map_out_name + '.png' + hdf_name
        common.info(my_log, log_tag, time_tag + msg)     
    except OSError,e:
        msg = 'rm ' + map_out_name + '.png.OK and ' + hdf_name + temp_ncl_log +' Failure!!!'       
        common.error(my_log, log_tag, time_tag + msg)
        return False
    
    return True


# main loop
def main():
    global my_table
    
    timeuse_begin = time.time()   
    msg = sat + ' : ' + ins +' : ' + 'export ' + ymd +' global data start now !'
    common.info(my_log, log_tag, time_tag + msg)
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, time_tag +'program start. pid=' + str(pid))
    
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)  
    
    #--sat=mysat --ins=myins --date=mydate --flag=myflag
    # check ps result, kill previous same program, avoiding hang.
    # we do NOT grep --date=2014-04-27-18 for convenience.
    cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + conf.bin_path + ' | ' \
        + conf.grep + ' -v grep | ' + conf.grep + ' -v tail | ' + conf.grep + ' ' \
        + ' -v bash | ' + conf.grep + ' ' + fname + ' | ' + conf.grep \
        + " '\-\-sat=" + arguments['--sat'] + "' |  " + conf.grep + " '\-\-ins=" + arguments['--ins'] + "' | " \
        + conf.awk + " '{print $4}'"
    (status, value) = commands.getstatusoutput(cmd)
    pid_list = value.split()
    for one_pid in pid_list:
        if int(one_pid) != pid:
            msg = 'more then one prog find, kill old same prog[' + one_pid + ']'
            common.err(my_log, log_tag, time_tag + msg)
            cmd = conf.kill + ' -kill ' + one_pid
            commands.getstatusoutput(cmd)
    
    #get the useful table name
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.table_setting[sat.lower()][ins.lower()]['data_db'])
        #conn.select_db('FY3C_MWTS')
        #cur.execute('show tables like \'%' + ymd + '%033KM_MS\'')
        msg = 'show tables like \'%' + ymd + '%KM_MS_T639\''
        common.info(my_log, log_tag, time_tag + msg)
        cur.execute('show tables like \'%' + ymd + '%KM_MS_T639\'') # the result is already sorted by ascii.
        all_tables = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
        common.err(my_log, log_tag, time_tag + msg)
        sys.exit(3)
        
    #print all_tables
    #print len(all_tables)   
    if len(all_tables)<=0:
        msg = '1. no table found for time ' + ymd + ', please reset time span.'
        common.err(my_log, log_tag, time_tag + msg)
        sys.exit(4)

    my_table = []
    for one_table in all_tables:
        table_ymdh = one_table[0][19:30] 
        timeArray = time.strptime(table_ymdh + ':00:00', "%Y%m%d_%H:%M:%S")
        timeStamp = time.mktime(timeArray)
        if timeStamp >= my_ts['min'] and timeStamp <= my_ts['max']:
            my_table.extend([one_table[0]])
                
    if len(my_table)<=0:
        msg = '2. no table found for suite time ' + ymd + ', please reset time span.'
        common.err(my_log, log_tag, time_tag + msg)
        sys.exit(4)
        
    print my_table
    #call make hdf file function
    channeltupe=[]
    for channel in range(1,channels+1):
        channeltupe.append(channel)
    
  
    #create hdf file    
    pool = Pool(3)
    ret = pool.map(cal_and_create_hdf,channeltupe)
    pool.close()
    pool.join()
    if False in ret:
        msg = 'FAILED`some create hdf file error.`timeuse='
    else:
        msg = 'SUCC`some create hdf file finish.`timeuse='  
    
    if just_calc is True:
        return True

    #dram picture 
    #pool = ThreadPool(20) 
    pool = Pool(3)
    pool.map(drawglobalmap,channeltupe)
    pool.close()
    pool.join()
    
    if False in ret:
        msg = 'FAILED`some picture may NOT draw.`timeuse='
    else:
        msg = 'SUCC`some picture draw ok.This program use time `timeuse='   
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    common.info(my_log, log_tag, time_tag + msg + timeuse)
 
          
if __name__ == '__main__':
    main()


