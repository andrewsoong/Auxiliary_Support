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
# /usr/bin/python /home/f3cmon/MONITORFY3C/py2/bin/calc_draw_obc.py 
# --sat=fy3c --ins=mwts --nwp=t639 --date=now
# >> /home/f3cmon/DATA/LOG/calc_draw_obc.py.log 2>&1 &
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
from datetime import datetime
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 

warnings.filterwarnings('ignore', category = MySQLdb.Warning)

timeuse_begin = time.time()

sys.path.append('/home/f3cmon/MONITORFY3C/py2/conf')
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

# create hdf by obc setting
def create_obc_hdf(filename, numpy_data, obc_setting):
    try:
        hfile = h5.File(filename, 'w') # w: rewrite if hdf already exist.
        # time data is stored first as default.
        ymdh_arr = numpy.array(map(common.time_to_arr, numpy_data[:, 0]) )
        hfile.create_dataset("time", data = ymdh_arr.astype(numpy.int32))
        
        previous_idx = 0 # time idx = 0
        for one_setting in obc_setting:
            start_idx = previous_idx + 1
            for one_column in xrange(1, one_setting['columns'] + 1):
                if one_setting['columns'] == 1:
                    ds_name = one_setting['db_field']
                else:
                    ds_name = one_setting['db_field'] + str(one_column)
                
                idx = start_idx + one_column - 1
                factor = int(one_setting['factor'])
                
                if 'float' in one_setting['hdf_dtype'] \
                    and 'int' in one_setting['db_dtype']:
                    hfile.create_dataset(ds_name, data = \
                            numpy_data[:, idx].astype(numpy.float32)*1/factor)
                elif 'int' in one_setting['hdf_dtype'] \
                    and 'int' in one_setting['db_dtype']:
                    hfile.create_dataset(ds_name, data = \
                            numpy_data[:, idx].astype(numpy.int32))
                elif 'float' in one_setting['hdf_dtype'] \
                    and 'float' in one_setting['db_dtype']:
                    hfile.create_dataset(ds_name, data = \
                            numpy_data[:, idx].astype(numpy.float32))
                else:
                    pass
                
                previous_idx += 1
        
        hfile.close()
    except:
        return False
    
    return True

def draw_one_channel(channel):
    tmpfile = conf.tmp_path + '/' + my_tag + '.' + common.utc_YmdHMS() \
            + '.ch' + format(channel, '02d')

    sql = common.get_obc_3dim_sql(ins_conf.obc_3dim_to_db.values(),str(channel),
                                  my_channel_table, conf.obc_select_prefix_sql,
                                  conf.obc_3dim_where_sql) \
        + conf.export_txt%(tmpfile + '.txt')

    begin_sql = conf.obc_select_prefix_sql + ' 1 from ' + my_channel_table[0] \
                + conf.obc_3dim_where_sql + str(channel) + " limit 1"

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])      
        cur.execute(sql)
        cur.execute(begin_sql)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 3-dim ch' + str(channel) + ' png`Mysql Fatal Error[' \
            + str(e.args[0]) + ']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False

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
    tmphdf = tmpfile + '.' + png_title + '.HDF'

    # trans txt result to numpy fmt, to easy hdf create.
    all_data = numpy.loadtxt(tmpfile + '.txt', dtype='str', delimiter=',')
    
    ret = create_obc_hdf(tmphdf, all_data, ins_conf.obc_3dim_to_db.values())
    
    if not ret:
        return False
    return

    ret = draw_channel(tmphdf, format(channel,'02d'), png_title,begin_data[0]['ymdhms'].strftime("%Y%m%d"),\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))
    
    return True

def draw_channel(tmphdf, channel, png_title, begin_time, end_time):
    cmd = []
    
    cold_hot_tmp_png = conf.plot_path + '/'+ png_title + '_cold_hot_cnt_avg'
    cold_hot_ncl = conf.plot_path + '/obc_cold_hot_cnt.ncl'
    cold_hot_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.cold_hot.log'
    cmd_cold_hot = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"' channel=" + str(channel) \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + cold_hot_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  cold_hot_ncl \
        + ' > ' + cold_hot_log + ' 2>&1'
    print cmd_cold_hot
    cmd.append(cmd_cold_hot)
#    (status, output) = commands.getstatusoutput(cmd_cold_hot)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_cold_hot + '`' + output)
#     print 'The first sucess!!!!'

    argc_tmp_png = conf.plot_path + '/'+ png_title + '_argc'
    argc_ncl = conf.plot_path + '/obc_agc.ncl'
    argc_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.argc.log'
    cmd_argc = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"' channel=" + str(channel) \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + argc_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  argc_ncl \
        + ' > ' + argc_log + ' 2>&1'
    print cmd_argc
    cmd.append(cmd_argc)
#     (status, output) = commands.getstatusoutput(cmd_argc)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_argc + '`' + output)
#     print 'The second is sucess!!!!'

    cal_coef_tmp_png = conf.plot_path + '/'+ png_title + '_cal_coef'
    cal_coef_ncl = conf.plot_path + '/obc_cal_coef.ncl'
    cal_coef_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.cal_coef.log'
    cmd_cal_coef = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"' channel=" + str(channel) \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + cal_coef_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  cal_coef_ncl \
        + ' > ' + cal_coef_log + ' 2>&1'
    print cmd_cal_coef
    cmd.append(cmd_cal_coef)
#     (status, output) = commands.getstatusoutput(cmd_cal_coef)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_cal_coef + '`' + output)
#     print 'The thread is sucess!!!'

    print cmd
    #use map:16.6s ; not use map:44.52s
    timeuse_begin = time.time()
    pooltest = ThreadPool()
    ret = pooltest.map(commands.getstatusoutput, cmd )
    pooltest.close()
    pooltest.join()

    return

    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    print timeuse
    
    del_dict = {
                0:{'dir': conf.png_type['cold_hot_cnt_avg'], 'tmp_png':cold_hot_tmp_png, 'ncl_log': cold_hot_log},
                1:{'dir': conf.png_type['agc'], 'tmp_png':argc_tmp_png, 'ncl_log': argc_log},
                2:{'dir': conf.png_type['cal_coef'], 'tmp_png':cal_coef_tmp_png, 'ncl_log': cal_coef_log},
                }
    sds_len = len(del_dict)
    for i in xrange(0, sds_len): 
        # check png.OK
        if not common.check_file_exist(del_dict[i]['tmp_png'] + '.png', check_ok = True):
            msg = 'ncl program error: output png file not exist.' + del_dict[i]['tmp_png']
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False

        dest_path = conf.img_nsmc + '/' + sat.upper() + '/' + ins.upper() + '/' \
                  + del_dict[i]['dir'] + '/' 
        arch_path = dest_path + str(end_time[0:4]) + '/'
        latest_path = dest_path + 'LATEST/' + str(channel) + '/'
        print del_dict[i]['tmp_png'] + '.png'
        print latest_path + png_title + '.png'
        try:
            shutil.copyfile(del_dict[i]['tmp_png'] + '.png', arch_path + png_title + '.png')
            common.empty_folder(latest_path)
            common.mv_file(del_dict[i]['tmp_png'] + '.png', latest_path + png_title + '.png')
            os.remove(del_dict[i]['tmp_png'] + '.png.OK')
            os.remove(del_dict[i]['ncl_log'])
        except:
            msg = 'png created, but cp or mv to dest error'
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False
        
        try:
            os.remove(tmphdf)
            os.remove(tmphdf + '.txt')
        except OSError,e:
            msg = 'clean tmp file error[' + str(e.args[0])+']: ' + e.args[1]              
            common.warn(my_log, log_tag, time_tag + msg)

    return True
   

def draw_just_obc():
    tmpfile = conf.tmp_path + '/' + my_tag + '.' + common.utc_YmdHMS() + '.obc'

    sql = common.get_obc_2dim_sql(ins_conf.obc_to_db.values(),ins_conf.channels,
                                  my_obc_table, conf.obc_select_prefix_sql) \
        + conf.export_txt%(tmpfile + '.txt')
 
    begin_sql = conf.obc_select_prefix_sql + ' 1 from ' + my_obc_table[0] \
                + " limit 1"
         
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor(MySQLdb.cursors.DictCursor)
        conn.select_db(conf.table_setting[sat][ins]['data_db'])      
        cur.execute(sql)
        cur.execute(begin_sql)
        begin_data = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        # do NOT exit in thread!! To avoid zombie process.
        msg = 'draw obc 2-dim png`Mysql Fatal Error[' + str(e.args[0]) \
            + ']: ' + e.args[1]              
        common.err(my_log, log_tag, time_tag + msg)
        return False

    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_[PRT|INS_TEMP|...]
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + begin_data[0]['ymdhms'].strftime("%Y%m%d") + '_' \
                + begin_data[0]['ymdhms'].strftime("%H%M") + '_TO_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
                + '_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
                + '_' \
                + format(int(hour_span), '02d') + 'H_'
    tmphdf = tmpfile + '.' + png_title + '.HDF'

    # trans txt result to numpy fmt, to easy hdf create.
    all_data = numpy.loadtxt(tmpfile + '.txt', dtype='str', delimiter=',')
    
    ret = create_obc_hdf(tmphdf, all_data, ins_conf.obc_to_db.values())
    if not ret:
        return False

    ret = draw_nochannel(tmphdf, png_title,begin_data[0]['ymdhms'].strftime("%Y%m%d"),\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))

    return True
    
def draw_nochannel(tmphdf, png_title, begin_time, end_time):    
    cmd = []
    cold_ang_tmp_png = conf.plot_path + '/'+ png_title + 'cold_ang'
    cold_ang_ncl = conf.plot_path + '/obc_cold_ang.ncl'
    cold_ang_log = conf.tmp_path + '/monitor.' + log_tag +'.cold_ang.log'
    
    timeuse_begin = time.time()
    cmd_cold_ang = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"" +'\'' \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + cold_ang_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  cold_ang_ncl \
        + ' > ' + cold_ang_log + ' 2>&1'
    #print cmd_cold_ang
    cmd.append(cmd_cold_ang)
#     (status, output) = commands.getstatusoutput(cmd_cold_ang)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_cold_ang + '`' + output)

    hot_ang_tmp_png = conf.plot_path + '/'+ png_title + 'hot_ang'
    hot_ang_ncl = conf.plot_path + '/obc_hot_ang.ncl'
    hot_ang_log = conf.tmp_path + '/monitor.' + log_tag +'.hot_ang.log'
    
    cmd_hot_ang = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"" +'\'' \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + hot_ang_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  hot_ang_ncl \
        + ' > ' + hot_ang_log + ' 2>&1'
    #print cmd_hot_ang
    cmd.append(cmd_hot_ang)
#     (status, output) = commands.getstatusoutput(cmd_hot_ang)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_hot_ang + '`' + output)

    earth_ang_tmp_png = conf.plot_path + '/'+ png_title + 'earth_ang'
    earth_ang_ncl = conf.plot_path + '/obc_earth_ang.ncl'
    earth_ang_log = conf.tmp_path + '/monitor.' + log_tag +'.earth_ang.log'
    
    cmd_earth_ang = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"" +'\'' \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + earth_ang_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  earth_ang_ncl \
        + ' > ' + earth_ang_log + ' 2>&1'
#    print cmd_earth_ang
    cmd.append(cmd_earth_ang)
#     (status, output) = commands.getstatusoutput(cmd_earth_ang)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_earth_ang + '`' + output)


    ins_tmp_png = conf.plot_path + '/'+ png_title + 'ins_temp'
    ins_temp_ncl = conf.plot_path + '/obc_ins_temp.ncl'
    ins_temp_log = conf.tmp_path + '/monitor.' + log_tag +'.ins_temp.log'
    
    cmd_ins_temp = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"" +'\'' \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + ins_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  ins_temp_ncl \
        + ' > ' + ins_temp_log + ' 2>&1'
#    print cmd_ins_temp
    cmd.append(cmd_ins_temp)
#     (status, output) = commands.getstatusoutput(cmd_ins_temp)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_ins_temp + '`' + output)


    prt_tmp_png = conf.plot_path + '/'+ png_title + 'prt'
    prt_ncl = conf.plot_path + '/obc_prt.ncl'
    prt_log = conf.tmp_path + '/monitor.' + log_tag +'.prt.log'
    
    cmd_prt = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
        + "'instrument=\"" + ins.upper() + "\"" +'\'' \
        + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + prt_tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " +  prt_ncl \
        + ' > ' + prt_log + ' 2>&1'
#    print cmd_prt
    cmd.append(cmd_prt)
#     (status, output) = commands.getstatusoutput(cmd_prt)
#     common.debug(my_log, log_tag, str(status) + '`' + cmd_prt + '`' + output)

    print cmd
    timeuse_begin = time.time()
    pooltest = ThreadPool()
    ret = pooltest.map(commands.getstatusoutput, cmd )
    pooltest.close()
    pooltest.join()
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin, 2))
    print timeuse


    del_dict = {
                0:{'dir': conf.png_type['cold_ang'], 'tmp_png':cold_ang_tmp_png, 'ncl_log': cold_ang_log},
                1:{'dir': conf.png_type['hot_ang'], 'tmp_png':hot_ang_tmp_png, 'ncl_log': hot_ang_log},
                2:{'dir': conf.png_type['earth_ang'], 'tmp_png':earth_ang_tmp_png, 'ncl_log': earth_ang_log},
                3:{'dir': conf.png_type['ins_temp'], 'tmp_png':ins_tmp_png, 'ncl_log': ins_temp_log},
                4:{'dir': conf.png_type['prt'], 'tmp_png':prt_tmp_png, 'prt_png':prt_tmp_png, 'ncl_log': prt_log},
                }
    sds_len = len(del_dict)
    print sds_len
    for i in xrange(0, sds_len):  
        # check png.OK
        if not common.check_file_exist(del_dict[i]['tmp_png'] + '.png', check_ok = True):
            msg = 'ncl program error: output png file not exist.' + cold_hot_tmp_png
            common.error(my_log, log_tag, time_tag + msg)
            return False
     
        dest_path = conf.img_nsmc + '/' + sat.upper() + '/' + ins.upper() + '/' \
                  + del_dict[i]['dir'] + '/' 
        arch_path = dest_path + str(end_time[0:4]) + '/'
        latest_path = dest_path + 'LATEST/'  
        
        try:
            shutil.copyfile(del_dict[i]['tmp_png'] + '.png', arch_path + png_title + '.png')
            common.empty_folder(latest_path)
            common.mv_file(del_dict[i]['tmp_png'] + '.png', latest_path + png_title + '.png')
            os.remove(del_dict[i]['tmp_png'] + '.png.OK')
            os.remove(del_dict[i]['ncl_log'])
        except:
            msg = 'png created, but cp or mv to dest error'
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False

    try:
        os.remove(tmphdf)
        os.remove(tmphdf + '.txt')
    except OSError,e:
        msg = 'clean tmp file error[' + str(e.args[0])+']: ' + e.args[1]              
        common.warn(my_log, log_tag, time_tag + msg)
    
    return True


def draw_obc(input):
    #for test!!!!!
    if input != 1:
        return
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
    all_obc_table = [ x for x in all_tables if 'OBCXX' in x[0] ]
    channel_tag = 'OBCXX_MS_' + str(ins_conf.channels)
    channel_table = [ x for x in all_obc_table if channel_tag in x[0]]
    obc_table = list(set(all_obc_table).difference(set(channel_table))) #return in all_obc_table but no in channel_table
     
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
            
    if len(my_channel_table)<=0:
        msg = time_tag + 'no table found for 3-dims data'
        common.info(my_log, log_tag, msg)
        sys.exit(4)
    
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

