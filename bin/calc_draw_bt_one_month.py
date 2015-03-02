#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, calculate bt avg(obs-sim), STDDEV_POP(obs-sim), etc.
every 6 or 12 hours, insert to db, and draw time series.

Usage:
    calc_draw_bt.py --sat=fy3c --ins=mwts --nwp=t639 --span=6 --date=now
    --just_calc=false --just_draw=false --save_hdf=false

Arguments:
    sat: the satellite you want to calc, fy3c
    ins: the insatrument you want to calc, mwts
    nwp: t639 or ncep
    span: hour span, 6 or 12. ONLY 6 is supported now.
    date: calc for special date. YYYY-mm-dd-h like 2014-04-24-19 [default: now]
        and, 2014-04-24-19 means calc for 12:00-->18:00, and, save result to
        stat table ymdh = 2014-04-24 18:00:00, means 12:00-->18:00
    just_calc: true or false. false as default. if true, we do NOT draw png.
    just_draw: true: only draw png, do NOT calc bt to db.
    save_hdf: true: after draw png, save hdf in tmp path.

eg:
if we are crontabed at 2013-12-06 04:00, we should calc for previous time zone 
(04:00-6) = -02:00, that is: (yesterday)18:00-->23:59's data, and , save 
result to stat table ymdh = 2013-12-06 00:00, means yesterday 18:00-->23:59.

Why? if we just calc for cur time zone, 2013-12-06 00:00 to 03:59, the HDF data 
with time FY3C_MWTSX_GBAL_20131206_0355*** may NOT be arrived for some
acceptable reason. such as redo in COSS, or network reason. 

mysql table like:
	create table *_6H[|12H](ymdh datetime, sim_mod char(5), channel int, 
	num int, avg float, std float)
"""

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
# crontabed every 6 or 12 hours, then
#      check ps result, kill previous same program, avoiding hang.
#      get time span
#      get calc sql
#      run sql, save calc result to STAT db
#      export all life time series data to hdf
#      draw png
#      mv png to dest path
#         
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/calc_draw_bt.py 
# --sat=fy3c --ins=mwts --nwp=t639 --span=6 --date=now --just_calc=false
# --just_draw=false --save_hdf=false
# >> /home/fymonitor/DATA/LOG/calc_draw_bt.py.log 2>&1
#                         
# date          author    changes
# 2014-07-25    gumeng    add just_draw, save_hdf
# 2014-06-25    gumeng    bug fix: avg(o-b) is None, we should set 0 as default
# 2014-06-18    guemng    add just_calc flag for calc only.
# 2014-06-16    guemng    change latest_path with %02d
# 2014-05-30    gumeng    fix bug for more then one prog find
# 2014-04-28	gumeng	  export mysql data to txt, then load as hdf.
# 2014-04-23    gumeng    create

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
import datetime
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
hour_span = arguments['--span'].lower()
calc_date = arguments['--date'].lower()
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

#get the correct time span.
if calc_date == 'now':
    #calc_date = common.utc_YmdH()
    calc_date = datetime.datetime.now() + datetime.timedelta(days=-3)
    calc_date = calc_date.strftime('%Y-%m-%d-%H')
#calc_date ='2014-08-13-03'
print calc_date
print hour_span
timespan = common.get_calc_timespan(calc_date, hour_span)
print timespan

# timespan = common.get_last_mon_calc_timespan(timespan)
time_tag = timespan['begin_str'] + '`' + timespan['end_str'] + '`'
print time_tag
#time_tag = '2014-07-31 18:00:00`2014-09-01 00:00:00`'

common.wt_file(my_pidfile, str(pid))
common.info(my_log, log_tag, time_tag + 'program start')

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, time_tag + msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

def	draw_one_channel(channel):
    tmpfile = conf.tmp_path + '/' + my_tag + '.' + common.utc_YmdHMS() \
			+ '.ch' + format(channel, '02d')

    # do NOT select sim_mod, channel for easy export to txt, but select's
    # result is sorted same with select sim_mod
    sql = 'select ymdh, num, avg, std from ' + stat_table \
		+ " where ymdh like '%2014-11%' and channel=" + str(channel) + conf.export_txt%(tmpfile + '.txt')
    print sql
    
    begin_sql = 'select ymdh from ' + stat_table + " where ymdh like '%2014-11%' and channel=" \
    			+ str(channel) + " limit 1"
    end_sql = 'select ymdh from ' + stat_table + " where ymdh like '%2014-11%' and channel=" \
    			+ str(channel) + " order by ymdh desc limit 1"
    print end_sql
# 	select * from FY3C_MWTS_BT_6H where channel=1;
# 	| ymdh                | sim_mod | channel | num    | avg     | std     |
# 	+---------------------+---------+---------+--------+---------+---------+
# 	| 2014-03-01 06:00:00 | crtm    |       1 | 376302 | 15.4548 |   17.47 |
# 	| 2014-03-01 06:00:00 | rttov   |       1 | 392389 | 5.05573 | 15.3406 |
# 	| 2014-03-01 12:00:00 | crtm    |       1 | 930282 | 12.7587 | 17.0431 |
# 	| 2014-03-01 12:00:00 | rttov   |       1 | 970205 | 4.54543 | 14.8319 |
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
		cur.execute(end_sql)
		end_data = cur.fetchall()
		cur.close()
		conn.close()
    except MySQLdb.Error, e:
		# do NOT exit in thread!! To avoid zombie process.
		msg = 'channel ' + str(channel) + '`Mysql Fatal Error[' \
			+ str(e.args[0])+']: ' + e.args[1]			  
		common.err(my_log, log_tag, time_tag + msg)
		return False
 	
    # begin_data in db is end edge of calc span [begin, end]
    # so we need get correct begin data of start edge.
    begin_datetime = begin_data[0]['ymdh'] + timedelta(hours= 0 - int(hour_span))

	# like: FY3C_MWTS_20131014_0000_TO_2013123_2318_CH01_06H
    png_title = sat.upper() + '_' + ins.upper() + '_' \
				+ begin_datetime.strftime("%Y%m%d_%H%M") + '_TO_' \
				+ end_data[0]['ymdh'].strftime("%Y%m%d_%H%M") + '_CH' \
				+ format(channel, '02d')+'_'+format(int(hour_span), '02d') +'H'
    tmphdf = tmpfile + '.' + png_title + '.HDF'
    print tmphdf

	# trans txt result to numpy fmt, to easy hdf create.
    all_data = numpy.loadtxt(tmpfile + '.txt', dtype='str', delimiter=',')
    hfile = h5.File(tmphdf, 'w') # w: rewrite if hdf already exist.
	# we need odd lines.
    ymdh_arr = numpy.array(map(common.time_to_arr, all_data[::2 ,0]) )
    hfile.create_dataset("time", data = ymdh_arr.astype(numpy.int32))
    hfile.create_dataset("rttov_num", data=all_data[1::2, 1].astype(numpy.int32))
    hfile.create_dataset("crtm_num", data=all_data[::2, 1].astype(numpy.int32))
    hfile.create_dataset("rttov_avg", data=all_data[1::2, 2].astype(numpy.float32))
    hfile.create_dataset("crtm_avg", data=all_data[::2, 2].astype(numpy.float32))
    hfile.create_dataset("rttov_std", data=all_data[1::2, 3].astype(numpy.float32))
    hfile.create_dataset("crtm_std", data=all_data[::2, 3].astype(numpy.float32))
    hfile.close()
    
	# call NCL, check png.OK file, mv to dest...
    # cmd = ncl curve_map.ncl 'sat="FY3C"' 'instrument="MWTS"' channel=1 
    # BG_YMD=20140301 ED_YMD=20140310 'fils_in=".HDF"' 'file_out="a"' 
    # 'file_title="FY3C_MWTS_20140301_0000_TO_20140301_0600_CH01_06H"'
    tmp_png = tmpfile + '.' + png_title
    cmd = conf.ncl + ' ' + conf.draw_bt + " 'sat=\"" + sat.upper() + "\"' " \
        + " 'instrument=\"" + ins.upper() + "\"' channel=" + str(channel) \
        + ' BG_YMD=' + begin_datetime.strftime("%Y%m%d") \
        + ' ED_YMD=' + end_data[0]['ymdh'].strftime("%Y%m%d") \
        + " 'file_in=\"" + tmphdf + "\"' " \
        + " 'file_out=\"" + tmp_png + "\"' " \
        + " 'file_title=\"" + png_title + "\"' " \
        + ' > ' + tmpfile + '.log' + ' 2>&1'
    (status, output) = commands.getstatusoutput(cmd)

    common.debug(my_log, log_tag, str(status) + '`' + cmd + '`' + output)
    # check png.OK
    if not common.check_file_exist(tmp_png + '.png', check_ok = True):
        msg = 'ncl program error: output png file not exist.'
        common.error(my_log, log_tag, time_tag + msg)
        return False
        
    # mv png to img path.
    dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/' + sat.upper() + '/' + ins.upper() + '/' \
              + conf.png_type['bt'] + '/' 
    arch_path = dest_path + end_data[0]['ymdh'].strftime("%Y") + '/' \
                + end_data[0]['ymdh'].strftime("%m") + '/'
    latest_path = dest_path + 'LATEST/' + format(channel, '02d') + '/'
    print latest_path
    try:
        shutil.copyfile(tmp_png + '.png', arch_path + png_title + '.png')
        common.empty_folder(latest_path)
        common.mv_file(tmp_png + '.png', latest_path + png_title + '.png')
    except (OSError, IOError) as e:
        msg = 'channel ' + format(channel, '02d') + ' png created, but cp ' \
            + 'or mv to dest error[' + str(e.args[0])+']: ' + e.args[1] 
        print msg
        common.error(my_log, log_tag, time_tag + msg)
        return False

    try:
        os.remove(tmp_png + '.png.OK')
        if not save_hdf:
            os.remove(tmphdf)        
        os.remove(tmpfile + '.txt')
        os.remove(tmpfile + '.log')
    except OSError,e:
        msg = 'channel ' + format(channel, '02d') +' clean tmp file error[' \
            + str(e.args[0])+']: ' + e.args[1]              
        common.warn(my_log, log_tag, time_tag + msg)
    
    return True

        
def calc_one_channel(channel):
    rttov_sql = common.get_sql_calc_stat(channel,ins_conf.sql_rttov_bt,my_table)
    common.debug(my_log, log_tag, 'channel '+str(channel)+'`rttov`'+rttov_sql)

    crtm_sql = common.get_sql_calc_stat(channel, ins_conf.sql_crtm_bt,my_table)
    common.debug(my_log, log_tag, 'channel '+str(channel)+'`crtm`'+crtm_sql)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
							 user=conf.db_setting['master']['user'],
							 passwd=conf.db_setting['master']['pwd'], 
							 port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(rttov_sql)
        ret = cur.fetchall()
        if ret[0][1] is None:
            avg = 0
        else:
            avg = ret[0][1]
        if ret[0][2] is None:
            std = 0
        else:
            std = ret[0][2]
        rttov_data = ('rttov', channel, ret[0][0], avg, std) 
        cur.execute(crtm_sql)
        ret = cur.fetchall()
        if ret[0][1] is None:
            avg = 0
        else:
            avg = ret[0][1]
        if ret[0][2] is None:
            std = 0
        else:
            std = ret[0][2]
        crtm_data = ('crtm', channel, ret[0][0], avg, std) 
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
	
def calc_and_draw_one_channel(channel):
    print channel
    if just_draw:
        return draw_one_channel(channel)
    
    if just_calc:
        return calc_one_channel(channel)

    # calc and draw.    
    if not calc_one_channel(channel):
        return False
    else:
        return draw_one_channel(channel)
    
        
# register signal function.
signal.signal(signal.SIGTERM, signal_handler)   
signal.signal(signal.SIGINT, signal_handler)      

# make sure ONLY ONE prog exist. kill other same program, avoiding hang.
# we do NOT grep --date=2014-04-27-18 for convenience.
cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + conf.bin_path + ' | ' \
	+ conf.grep + ' -v grep | ' + conf.grep + ' -v tail | ' + conf.grep + ' ' \
	+ ' -v bash | ' + conf.grep + ' ' + fname + ' | ' + conf.grep \
	+ " '\-\-sat=" + sat + "' | " + conf.grep + " '\-\-ins=" + ins + "' | " \
	+ conf.grep + " '\-\-nwp=" + nwp + "' | " + conf.grep + " '\-\-span=" \
	+ hour_span + "' | " + conf.awk + " '{print $4}'"
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
	msg = 'FAILED`Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
	common.err(my_log, log_tag, time_tag + msg)
	sys.exit(3)
	
# ignore obc table.
l1b_table = [ x for x in all_tables if nwp.upper() in x[0] ]

my_table = []
for idx, one_table in enumerate(l1b_table):
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
		my_table.extend([one_table[0]])
    elif timespan['begin_t'] > cur_timeStamp \
        and timespan['begin_t'] < next_timeStamp:
		my_table.extend([one_table[0]])
    elif timespan['begin_t'] < cur_timeStamp \
        and timespan['end_t'] > cur_timeStamp:
		my_table.extend([one_table[0]])
    elif timespan['end_t'] == cur_timeStamp:
		break
print my_table

if len(my_table)<=0:
	common.err(my_log, log_tag, time_tag + 'FAILED`no table found')
	sys.exit(4)

# get insert sql to STAT db.
stat_table = conf.table_setting[sat][ins]['stat_' + hour_span + 'h']
print stat_table

insert_sql = 'replace into ' + stat_table + " values('"+timespan['end_str'] \
	    	+ "', %s, %s, %s, %s, %s)"

pool = Pool()
ret = pool.map(calc_and_draw_one_channel, xrange(1, ins_conf.channels + 1) )
pool.close()
pool.join()

if False in ret:
	msg = 'FAILED`some png may NOT draw.`timeuse='
else:
	msg = 'SUCC`program finish.`timeuse='
	
timeuse_end = time.time()
timeuse = str(round(timeuse_end - timeuse_begin, 2))
common.info(my_log, log_tag, time_tag + msg + timeuse)
