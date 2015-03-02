#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, draw OBC 2-dim and 3-dim data every 12 or 6 hours.

Usage:
    calc_draw_obc.py --sat=fy3c --ins=mwts --span=12 --date=now --delhdf=False

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
    if len(my_channel_table) <= 0:
        return True
    tmpfile = '/home/fymonitor/DATA/TEMPHDF' + '/' + my_tag + '.' + common.utc_YmdHMS() \
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
#     png_title = sat.upper() + '_' + ins.upper() + '_' \
#                 + begin_data[0]['ymdhms'].strftime("%Y%m%d") + '_' \
#                 + begin_data[0]['ymdhms'].strftime("%H%M") + '_TO_' \
#                 + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
#                 + '_' \
#                 + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
#                 + '_' \
#                 + format(int(hour_span), '02d') + 'H_CH' \
#                 + format(channel,'02d')
                
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + '20131001' + '_' \
                + '0000' + '_TO_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
                + '_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
                + '_' \
                + format(int(hour_span), '02d') + 'H_CH' \
                + format(channel,'02d')
               
                
    tmphdf = tmpfile + '.' + png_title + '.HDF'

    #FY3C_MWTS_LONGLIFE_CHANNEL_01.txt
    long_file = '/hds/assimilation/fymonitor/DATA/OUTPUT/TXT/' + sat.upper() + '/'\
                + ins.upper() +'/'+ sat.upper() + '_' + ins.upper()+ '_LONGLIFE_CHANNEL_'\
                + format(channel,'02d') + '.txt'
                     
    file_temp = '/hds/assimilation/fymonitor/DATA/OUTPUT/TXT/' + sat.upper() + '/'\
                + ins.upper() +'/'+ sat.upper() + '_' + ins.upper()+ '_LONGLIFE_CHANNEL_'\
                + format(channel,'02d') + '_Temp.txt'           
    ret = combine_txt(tmpfile + '.txt' , long_file, file_temp)
    if not ret:
        return False
    
    # trans txt result to numpy fmt, to easy hdf create.
    all_data = numpy.loadtxt(file_temp, dtype='str', delimiter=',')
    
    ret = create_obc_hdf(tmphdf, all_data, ins_conf.obc_3dim_to_db.values())
    
    if not ret:
        return False

#     ret = draw_channel(tmphdf, format(channel,'02d'), png_title,begin_data[0]['ymdhms'].strftime("%Y%m%d"),\
#                        datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))
    ret = draw_channel(tmphdf, format(channel,'02d'), png_title, "20131001",\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))
   
    return True

def draw_channel(tmphdf, channel, png_title, begin_time, end_time):
    cmd = []
    sds_len = len(conf.draw_ncl[ins]['ncl_prog_channel'])
    for i in xrange(0, sds_len): 
        file_out = conf.plot_path + '/'+ png_title + '_' + conf.draw_ncl[ins]['ncl_prog_channel'][i]['tmp_png']
        file_title = conf.plot_path +'/obc_' + conf.draw_ncl[ins]['ncl_prog_channel'][i]['tmp_png'] + '.ncl'
        temp_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.' + conf.draw_ncl[ins]['ncl_prog_channel'][i]['tmp_png'] +'.log'
        temp_cmd = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
            + "'instrument=\"" + ins.upper() + "\"' channel=" + str(channel) \
            + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
            + " 'file_in=\"" + tmphdf + "\"' " \
            + " 'file_out=\"" + file_out + "\"' " \
            + " 'file_title=\"" + png_title + conf.draw_ncl[ins]['ncl_prog_channel'][i]['dir_name'] + "\"' " +  file_title \
            + ' > ' + temp_log + ' 2>&1'
        print temp_cmd
        cmd.append(temp_cmd)

    print cmd
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
    
    sds_len = len(conf.draw_ncl[ins]['ncl_prog_channel'])
    for i in xrange(0, sds_len): 
        file_out = conf.plot_path + '/'+ png_title + '_' + conf.draw_ncl[ins]['ncl_prog_channel'][i]['tmp_png']
        file_title = conf.plot_path +'/obc_' + conf.draw_ncl[ins]['ncl_prog_channel'][i]['tmp_png'] + '.ncl'
        temp_log = conf.tmp_path + '/monitor.' + log_tag + '.ch' + channel+'.' + conf.draw_ncl[ins]['ncl_prog_channel'][i]['tmp_png'] +'.log'
        # check png.OK
        if not common.check_file_exist(file_out + '.png', check_ok = True):
            msg = 'ncl program error: output png file not exist.' + file_out
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False
        dest_path = conf.img_nsmc + '/' + sat.upper() + '/' + ins.upper() + '/' \
                  + conf.png_type[conf.draw_ncl[ins]['ncl_prog_channel'][i]['tmp_png']] + '/' 
        arch_path = dest_path + str(end_time[0:4]) + '/'
        latest_path = dest_path + 'LATEST/' + str(channel) + '/'
        
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
        
#     if arguments['--delhdf'] : 
#         try:
#             os.remove(tmphdf)
#             os.remove(tmphdf + '.txt')
#         except OSError,e:
#             msg = 'clean tmp file error[' + str(e.args[0])+']: ' + e.args[1]              
#             common.warn(my_log, log_tag, time_tag + msg)

    return True
   

def draw_just_obc():
    if len(my_obc_table) <= 0:
        return True
    tmpfile = '/home/fymonitor/DATA/TEMPHDF' + '/' + my_tag + '.' + common.utc_YmdHMS() + '.obc'
 
    sql = common.get_obc_2dim_sql(ins_conf.calc_to_db.values(),ins_conf.channels,
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
#     png_title = sat.upper() + '_' + ins.upper() + '_' \
#                 + begin_data[0]['ymdhms'].strftime("%Y%m%d") + '_' \
#                 + begin_data[0]['ymdhms'].strftime("%H%M") + '_TO_' \
#                 + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
#                 + '_' \
#                 + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
#                 + '_' \
#                 + format(int(hour_span), '02d') + 'H_'

    # like: FY3C_MWTS_20140303_0259_TO_20140428_1159_12H_[PRT|INS_TEMP|...]
    png_title = sat.upper() + '_' + ins.upper() + '_' \
                + '20131001' + '_' \
                + '0000' + '_TO_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d') \
                + '_' \
                + datetime.utcfromtimestamp(timespan['end_t']).strftime('%H%M') \
                + '_' \
                + format(int(hour_span), '02d') + 'H_'

                
    tmphdf = tmpfile + '.' + png_title + '.HDF'
    print tmphdf

#     long_file = '/hds/assimilation/fymonitor/DATA/OUTPUT/TXT/' + sat.upper() + '/'\
#                 + ins.upper() +'/'+ sat.upper() + '_' + ins.upper()+ '_LONGLIFE_OBC.txt'
#     file_temp = '/hds/assimilation/fymonitor/DATA/OUTPUT/TXT/' + sat.upper() + '/'\
#                 + ins.upper() +'/'+ sat.upper() + '_' + ins.upper()+ '_LONGLIFE_OBC_Temp.txt'            
#     ret = combine_txt(tmpfile + '.txt' , long_file, file_temp)
#     if not ret:
#         return False

    # trans txt result to numpy fmt, to easy hdf create.
    all_data = numpy.loadtxt(tmpfile + '.txt', dtype='str', delimiter=',')
    
    ret = create_obc_hdf(tmphdf, all_data, ins_conf.calc_to_db.values())
    if not ret:
        return False
    
    return

#     ret = draw_nochannel(tmphdf, png_title,begin_data[0]['ymdhms'].strftime("%Y%m%d"),\
#                        datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))
    ret = draw_nochannel(tmphdf, png_title,'20131001',\
                       datetime.utcfromtimestamp(timespan['end_t']).strftime('%Y%m%d'))

    return True


def combine_txt(tmpfile,long_file,file_temp):
    readsize = 1024*1024
    file = open(file_temp, "w+") 
    
    longobj  = open(long_file, 'r')
    while 1:
        filebytes = longobj.read(readsize)
        if not filebytes: 
            break
        file.write(filebytes) 
    longobj.close()
    
    tmpobj  = open(tmpfile, 'r')
    while 1:
        filebytes = tmpobj.read(readsize)
        if not filebytes: 
            break
        file.write(filebytes) 
    tmpobj.close()
    
    file.close()
    return True

def draw_nochannel(tmphdf, png_title, begin_time, end_time):    
    cmd = []
    sds_len = len(conf.draw_ncl[ins]['ncl_prog_no_channel'])
    for i in xrange(0, sds_len): 
        file_out = conf.plot_path + '/'+ png_title + conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['tmp_png']
        file_title = conf.plot_path +'/obc_' + conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['tmp_png'] + '.ncl'
        temp_log = conf.tmp_path + '/monitor.' + log_tag +'.' + conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['tmp_png'] +'.log'
        temp_cmd = conf.ncl + " 'sat=\"" + sat.upper() + "\"' " \
            + "'instrument=\"" + ins.upper() + "\"" +'\'' \
            + ' \'' +'BG_YMD=' + begin_time + '\' ' + ' \'' + 'ED_YMD=' + end_time + '\''\
            + " 'file_in=\"" + tmphdf + "\"' " \
            + " 'file_out=\"" + file_out + "\"' " \
            + " 'file_title=\"" + png_title + conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['dir_name'] + "\"' " +  file_title \
            + ' > ' + temp_log + ' 2>&1'
        #print temp_cmd
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

    sds_len = len(conf.draw_ncl[ins]['ncl_prog_no_channel'])
    for i in xrange(0, sds_len): 
        file_out = conf.plot_path + '/'+ png_title + conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['tmp_png']
        file_title = conf.plot_path +'/obc_' + conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['tmp_png'] + '.ncl'
        temp_log = conf.tmp_path + '/monitor.' + log_tag + '.' + conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['tmp_png'] +'.log'
        # check png.OK
        if not common.check_file_exist(file_out + '.png', check_ok = True):
            msg = 'ncl program error: output png file not exist.' + file_out
            print msg
            common.error(my_log, log_tag, time_tag + msg)
            return False
        dest_path = conf.img_nsmc + '/' + sat.upper() + '/' + ins.upper() + '/' \
                  + conf.png_type[conf.draw_ncl[ins]['ncl_prog_no_channel'][i]['tmp_png']] + '/' 
        arch_path = dest_path + str(end_time[0:4]) + '/'
        latest_path = dest_path + 'LATEST/' 
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

    print tmphdf
    if arguments['--delhdf'] : 
        try:
            os.remove(tmphdf)
            os.remove(tmphdf + '.txt')
        except OSError,e:
            msg = 'clean tmp file error[' + str(e.args[0])+']: ' + e.args[1]              
            common.warn(my_log, log_tag, time_tag + msg)
    
    return True


def draw_obc(input):
    #for test!!!!!
#     if input != 'just_obc':
#         return
    print input
    if input == 'just_obc':
        return draw_just_obc()
    else:
        return draw_one_channel(input)
 
def main():
    global my_channel_table
    global my_obc_table
    
    now_data = common.utc_nowdate()
    year_month = now_data[0:6]
    
    timeuse_begin_main = time.time()
    
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
    
    sql_cmd = 'show tables like \"FY3C_MWTSX_GBAL_L1_' + year_month +'%\" '
    #sql_cmd = 'show tables like \"FY3C_MWTSX_GBAL_L1_' + '201408' +'%\" '
    print sql_cmd
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
        #cur.execute('show tables ') # the result is already sorted by ascii.
        cur.execute('show tables like "FY3C_MWTSX_GBAL_L1_201407%" ') 
        #cur.execute(sql_cmd)
        all_tables = cur.fetchall()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = 'Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, time_tag + msg)
        sys.exit(3)
        
    # ignore L1B table.
    all_obc_table = [ x for x in all_tables if 'OBCXX' in x[0] ]
    OBC_CALC_tag = 'OBCXX_MS_CALC' 
    OBC_CALC_table = [ x for x in all_obc_table if OBC_CALC_tag in x[0]]    
    
    for idx, one_table in enumerate(OBC_CALC_table):
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
    
    draw_obc('just_obc')
    

    msg = 'SUCC`program finish.`timeuse='   
    timeuse_end = time.time()
    timeuse = str(round(timeuse_end - timeuse_begin_main, 2))
    print msg + timeuse
    common.info(my_log, log_tag, time_tag + msg + timeuse)

if __name__ == '__main__':
    main()

