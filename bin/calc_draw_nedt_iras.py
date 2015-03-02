#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, draw OBC 2-dim and 3-dim data every 12 or 6 hours.

Usage:
    calc_draw_nedt.py --sat=fy3c --ins=iras --span=12 --date=now

Arguments:
    sat: the satellite you want to calc, fy3c
    ins: the insatrument you want to calc, mwts
    span: hour span, 12 or 6. ONLY 12 is supported now.
    date: draw for special date. YYYY-mm-dd-h like 2014-04-24-12 [default: now]
        where 2014-04-24-12 means draw for launch time to today's 11:59
    defhdf: if set True, will del the export hdf file 

eg:
if we are crontabed at 2013-12-06 14:00, we should draw for previous time zone 
(14:00-12) = 02:00, that is: launch time to today's 11:59's data.
"""

__author__ = 'wzq'

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
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/calc_draw_nedt.py 
# --sat=fy3c --ins=mwts --nwp=t639 --date=now
# >> /home/fymonitor/DATA/LOG/calc_draw_nedt.py.log 2>&1 &
#                         
# date          author    changes
# 2014-08-02    wzq    create

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

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, time_tag + msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

def time_to_arr(data):
    return [data[0:4], data[5:7], data[8:10], \
              data[11:13], data[14:16]]

# create hdf by obc setting
def create_obc_hdf(filename, numpy_data):
    hfile = h5.File(filename, 'w')

    ymdh_arr = numpy.array(map(time_to_arr, numpy_data[:, 0]) )
    hfile.create_dataset("time", data = ymdh_arr.astype(numpy.int32))

    blackbody_nedt = hfile.create_dataset("blackbody_nedt",data=numpy_data[: ,1].astype(numpy.float32))
    coldspace_nedt = hfile.create_dataset("coldspace_nedt",data=numpy_data[: ,2].astype(numpy.float32))

    hfile.close()
    
    return True

def draw_one_channel(channel):

    tmpfile = '/home/fymonitor/DATA/TEMPHDF' + '/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d')
    print tmpfile
    #" INTO OUTFILE '%s' FIELDS TERMINATED BY ','"    
    # select ymdh , nedn from FY3C_MWTS_NEDN where channel=1;
    sql = 'select ymdh , blackbody_nedn, coldspace_nedn from ' + sat.upper() + '_' + ins.upper() + \
        '_NEDN where channel=' + str(channel) + ' and blackbody_nedn <5  and coldspace_nedn<5 INTO OUTFILE ' + '\'' \
        + tmpfile + '.txt' + '\'' + '  FIELDS TERMINATED BY \',\' '
    begin_sql = 'select ymdh , blackbody_nedn, coldspace_nedn from ' + sat.upper() + '_' + ins.upper() + \
        '_NEDN where channel=' + str(channel) + " and blackbody_nedn <5  and coldspace_nedn<5 limit 1"
    
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.db_setting['stat_db'])      
        cur.execute(sql)
        cur.execute(begin_sql)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        print 'Mysql error!!!!!!!!!!!'
        msg = 'draw nedt ch' + str(channel) + ' png`Mysql Fatal Error[' \
            + str(e.args[0]) + ']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False

    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_CH01_[PRT|INS_TEMP|...]
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + begin_data[0]['ymdh'].strftime("%Y%m%d") + '_' \
                + begin_data[0]['ymdh'].strftime("%H%M") + '_TO_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
                + '_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
                + '_' \
                + format(int(hour_span), '02d') + 'H_CH' \
                + format(channel,'02d')
    tmphdf = tmpfile + '.' + png_title + '.HDF'
    
    # trans txt result to numpy fmt, to easy hdf create.
    all_data = numpy.loadtxt(tmpfile + '.txt', dtype='str', delimiter=',')
    
    ret = create_obc_hdf(tmphdf, all_data)
    
    if not ret:
        return False

    ret = draw_channel(tmphdf, format(channel,'02d'), png_title,begin_data[0]['ymdh'].strftime("%Y%m%d"),\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'),'BLACKBODY')
    ret = draw_channel(tmphdf, format(channel,'02d'), png_title,begin_data[0]['ymdh'].strftime("%Y%m%d"),\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'),'COLDSPACE')
    return True

def draw_channel(tmphdf, channel, png_title, begin_time, end_time,sds_name):
    file_out = conf.plot_path + '/'+ png_title + '_' + sds_name +'_NEDN'
    file_title = conf.plot_path +'/FY3C_IRAS_'+ sds_name +'_NEdT.ncl'
    temp_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.' + sds_name +'_NEdT.log'
    temp_cmd = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"' channel=" + str(channel) \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + file_out + "\"' " \
        + " 'file_title=\"" + png_title + '_' + sds_name +'_NEdT' + "\"' " +  file_title \
        + ' > ' + temp_log + ' 2>&1'
    print temp_cmd


    (status, output) = commands.getstatusoutput(temp_cmd)
    common.debug(my_log, log_tag, str(status) + '`' + temp_cmd + '`' + output)
    

    if not common.check_file_exist(file_out + '.png', check_ok = True):
        msg = 'ncl program error: output png file not exist.' + file_out
        print msg
        common.error(my_log, log_tag, time_tag + msg)
        return False
    
    dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/' + sat.upper() + '/' + ins.upper() + '/' \
                  + sds_name + '_'+ 'NEDN' + '/' 
    arch_path = dest_path + str(end_time[0:4]) + '/'
    latest_path = dest_path + 'LATEST/' + str(channel) + '/'
    
    print dest_path
    print arch_path
    print latest_path
        
    try:
        shutil.copyfile(file_out + '.png', arch_path + png_title + '.png')
        common.empty_folder(latest_path)
        common.mv_file(file_out + '.png', latest_path + png_title + '.png')
        os.remove(file_out + '.png.OK')
        os.remove(temp_log)
    except:
        msg = 'png created, but cp or mv to dest error'
        print msg
        common.error(my_log, log_tag, time_tag + msg)
        return False
        
#     try:
#         os.remove(tmphdf)
#         os.remove(tmphdf + '.txt')
#     except OSError,e:
#         msg = 'clean tmp file error[' + str(e.args[0])+']: ' + e.args[1]              
#         common.warn(my_log, log_tag, time_tag + msg)

    return True
   
 
def main():
    global my_channel_table
    global my_obc_table
    
    timeuse_begin_main = time.time()
    
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, time_tag + 'program start')

    # register signal function.
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)      
   
   
    for channel in range(1,ins_conf.channels+1):
        draw_one_channel(channel)
    
    #draw_one_channel(3)    
    # create input for thread. 'just_obc' means draw 2-dim obc data
    # and, 1...13 means draw 3-dim obc data for each channel
#     pool = Pool(6)
#     ret = pool.map(draw_one_channel, range(1, ins_conf.channels + 1) )
#     pool.close()
#     pool.join()
    
#     if False in ret:
#         msg = 'FAILED`some png may NOT draw.`timeuse='
#     else:
#         msg = 'SUCC`program finish.`timeuse='
    msg = 'SUCC`program finish.`timeuse='   
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin_main, 2))
    print msg + timeuse
    common.info(my_log, log_tag, time_tag + msg + timeuse)

if __name__ == '__main__':
    main()

