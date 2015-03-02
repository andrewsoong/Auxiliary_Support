#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, for lat span, calculate bt avg(obs-sim), 
STDDEV_POP(obs-sim), etc. by each 12 or 6 hours. insert to db.
and, draw bt lat png.

Usage:
    calc_draw_lat.py --sat=fy3c --ins=mwts --nwp=t639 --lat_span=5
    --hour_span=12 --just_calc=false --just_draw=false --save_hdf=false
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

__author__ = 'gumeng'

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
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/calc_draw_lat.py 
# --sat=fy3c --ins=mwts --nwp=t639 --lat_span=5 --hour_span=12 --date=now
# >> /home/fymonitor/DATA/LOG/calc_draw_lat.py.log 2>&1
#                         
# date          author    changes
# 2014-06-25    gumeng    add save_hdf param for save hdf data in disk.
#
# 2014-06-25    gumeng    bug fix: avg(o-b) is None, we should set 0 as default
#
# 2014-05-23    gumeng    create

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

#get the correct time span.
if calc_date == 'now':
    #calc_date = common.utc_YmdH()
    calc_date = datetime.now() + timedelta(days=-3)
    calc_date = calc_date.strftime('%Y-%m-%d-%H')
print calc_date
timespan = common.get_calc_timespan(calc_date, hour_span)
# timespan = common.get_last_mon_calc_timespan(timespan)
time_tag = timespan['begin_str'] + '`' + timespan['end_str'] + '`'

# create latitude span list
factor = int(ins_conf.lat_sds_to_db[1]['factor'])
lat_list = common.get_data_with_span(-90*factor,90*factor,int(lat_span)*factor)

stat_table = conf.table_setting[sat][ins]['stat_' + hour_span + 'h_' \
                                          + lat_span + 'lat']
insert_sql = 'replace into ' + stat_table + " values('"+timespan['end_str'] \
            + "', %s, %s, %s, %s, %s, %s)"
my_table = []

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

    # do NOT select sim_mod, lat for easy export to txt, but select's
    # result is sorted same with select sim_mod, lat
    # Warning: 
    # we do NOT export to txt for time speedup, because
    # hdf time, lat sds is hard to change fmt to adjust NCL. bugfix later!!!
#     sql = 'select num, avg, std from ' + stat_table \
#         + " where channel=" + str(channel) + conf.export_txt%(tmpfile + '.txt')
    sql = 'select num, avg, std from ' + stat_table + " where channel=" \
        + str(channel) #+ ' order by sim_mod'
    
    sql_time = 'select ymdh from ' + stat_table + " where channel=" \
             + str(channel)+ " group by ymdh"

    begin_sql = 'select ymdh from ' + stat_table + " where channel=" \
    			+ str(channel) + " limit 1" # order by ymdh asc as default.
    end_sql = 'select ymdh from ' + stat_table + " where channel=" \
    			+ str(channel) + " order by ymdh desc limit 1"
                
# select * from FY3C_MWTS_BT_12H_5LAT where channel=1; without order by sim_mod
# | ymdh                | sim_mod | channel | lat | num   | avg        | std |
# | 2014-04-28 12:00:00 | crtm    |       1 | -85 |  6398 |    28.0772 | 6.06|
# | 2014-04-28 12:00:00 | crtm    |       1 | -80 | 26774 |    26.9812 | 8.52|
# ... ... ... ...
# | 2014-04-28 12:00:00 | rttov   |       1 | -85 |  6398 |   -13.1841 |  8.6|
# | 2014-04-28 12:00:00 | rttov   |       1 | -80 | 27206 |   -15.5067 | 11.6|
# ... ... ... ...
# | 2014-04-29 00:00:00 | crtm    |       1 | -85 |  6431 |    28.3289 | 5.85|
# | 2014-04-29 00:00:00 | crtm    |       1 | -80 | 26802 |    29.8865 | 8.73|
# ... ... ... ...
# | 2014-04-29 00:00:00 | rttov   |       1 | -85 |  6432 |   -11.2826 | 6.53|
# | 2014-04-29 00:00:00 | rttov   |       1 | -80 | 27246 |   -10.4366 | 12.1|
    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
							 user=conf.db_setting['master']['user'],
							 passwd=conf.db_setting['master']['pwd'], 
							 port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db(conf.db_setting['stat_db'])		
        cur.execute(sql)
        all_data= cur.fetchall()
        cur.execute(sql_time)
        time_data= cur.fetchall()
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
    begin_datetime = begin_data[0][0] + timedelta(hours= 0 - int(hour_span))
     
	# like: FY3C_MWTS_20131014_0000_TO_2013123_2318_CH01_06H
    png_title = sat.upper() + '_' + ins.upper() + '_' \
				+ begin_datetime.strftime("%Y%m%d_%H%M") + '_TO_' \
				+ end_data[0][0].strftime("%Y%m%d_%H%M") + '_CH' \
				+ format(channel, '02d')+'_'+format(int(hour_span), '02d') \
                + 'H_' + lat_span + 'LAT_' # RTTOV, CRTM
    tmphdf = tmpfile + '.' + png_title + '.HDF'

    
# 	# trans txt result to numpy fmt, to easy hdf create.
#     all_data = numpy.loadtxt(tmpfile + '.txt', dtype='str', delimiter=',')
    f = h5.File(tmphdf, 'w') # w: rewrite if hdf already exist.
    
    # create [-90, -85, ..., 85, 90] lat, while -90 lat should fill with 0 
    # avg, std, num data for easy plot.
    lat_arr = numpy.arange(-90, 90 + 1, int(lat_span), dtype = numpy.int32)
    f.create_dataset("lat", data = lat_arr )
    
    # here is the BEST practice:
#     time_arr = numpy.array(map(common.get_time_int, zip(*time_data)[0]),
#                            dtype = numpy.int32 )
#     f.create_dataset("time", data = time_arr )

    data_cnt = len(all_data)
    lat_cnt = len(lat_arr)
    time_cnt = len(time_data)
    
    time_sds = f.create_dataset("time", (time_cnt + 1,), dtype='i')
    
    num_rttov = f.create_dataset("num_rttov", (lat_cnt, time_cnt + 1), dtype='i')
    num_crtm = f.create_dataset("num_crtm", (lat_cnt, time_cnt + 1), dtype='i')
    
    avg_rttov = f.create_dataset("avg_rttov", (lat_cnt, time_cnt + 1), dtype='f')
    avg_crtm = f.create_dataset("avg_crtm", (lat_cnt, time_cnt + 1), dtype='f')
    
    stdp_rttov = f.create_dataset("stdp_rttov", (lat_cnt, time_cnt + 1), dtype='f')
    stdp_crtm = f.create_dataset("stdp_crtm", (lat_cnt, time_cnt + 1), dtype='f')
    
    time_sds[0:-1] = map(common.get_time_int, zip(*time_data)[0])
    
    # add one more time for NCL.
    last_time = time_data[-1][0] + timedelta(hours=12)
    time_sds[-1] = common.get_time_int(last_time)
    
    rttov_offset = lat_cnt - 1

    # get data for every ymdh's -90 to 90 latitude.
    for idx_time in xrange(0, time_cnt):
        offset = idx_time * (lat_cnt - 1) * 2
        for idx_lat in xrange(1, lat_cnt):
            idx_crtm = idx_lat -1 + offset
            num_crtm[idx_lat, idx_time] = all_data[idx_crtm][0]
            num_rttov[idx_lat, idx_time] = all_data[idx_crtm + rttov_offset][0]
            avg_crtm[idx_lat, idx_time] = all_data[idx_crtm][1]
            avg_rttov[idx_lat, idx_time] = all_data[idx_crtm + rttov_offset][1]
            stdp_crtm[idx_lat, idx_time] = all_data[idx_crtm][2]
            stdp_rttov[idx_lat, idx_time] = all_data[idx_crtm +rttov_offset][2]

    f.close()

	# call NCL, check png.OK file, mv to dest...
    for sim_mod in ('CRTMX', 'RTTOV'): # CRTMX is suit for png.
        tmp_png = tmpfile + '.' + png_title + sim_mod 
        cmd = conf.ncl + ' ' + conf.draw_lat[sim_mod] + " 'sat=\"" \
            + sat.upper() + "\"' " + " 'instrument=\"" + ins.upper() \
            + "\"' channel=" + str(channel) \
            + " 'file_in=\"" + tmphdf + "\"' " \
            + " 'file_out=\"" + tmp_png + "\"' " \
            + " 'file_title=\"" + png_title + sim_mod + "\"' " \
            + ' > ' + tmpfile + '.log' + ' 2>&1'
        print cmd
        (status, output) = commands.getstatusoutput(cmd)
        common.debug(my_log, log_tag, str(status) + '`' + cmd + '`' + output)
        
        # check png.OK
        if not common.check_file_exist(tmp_png + '.png', check_ok = True):
            msg = 'ncl error: output png file ' + tmp_png + ' not exist.'
            common.error(my_log, log_tag, time_tag + msg)
            return False
            
        # mv png to img path.
        dest_path = '/hds/assimilation/fymonitor/DATA/IMG/NSMC/' + sat.upper() + '/' + ins.upper() + '/' \
                  + conf.png_type['lat'] + '/' 
        print dest_path
        arch_path = dest_path + end_data[0][0].strftime("%Y") + '/' \
                    + end_data[0][0].strftime("%m") + '/'
        latest_path = dest_path + 'LATEST/' + format(channel, '02d') + '/'
    
        try:
            shutil.copyfile(tmp_png + '.png', arch_path + png_title + sim_mod \
                            + '.png')
            
            # clean folder only one time.
            if 'CRTMX' in sim_mod:
                common.empty_folder(latest_path)
                
            common.mv_file(tmp_png + '.png', latest_path + png_title + sim_mod \
                           + '.png')
        except (OSError, IOError) as e:
            msg = 'channel ' + format(channel, '02d') + ' png created, but cp ' \
                + 'or mv to dest error[' + str(e.args[0])+']: ' + e.args[1] 
            common.error(my_log, log_tag, time_tag + msg)
            return False

    try:
        os.remove(tmpfile + '.' + png_title + 'CRTMX.png.OK')
        os.remove(tmpfile + '.' + png_title + 'RTTOV.png.OK')
        if not save_hdf:
            os.remove(tmphdf)
        os.remove(tmpfile + '.log')
    except OSError,e:
        msg = 'channel ' + format(channel, '02d') +' clean tmp file error[' \
            + str(e.args[0])+']: ' + e.args[1]        
        common.warn(my_log, log_tag, time_tag + msg)
    
    return True

def calc_one_channel(channel):
    rttov_sql = common.get_sql_calc_stat(channel, ins_conf.sql_rttov_bt_lat,
                                         my_table)
    common.debug(my_log, log_tag, 'channel '+str(channel)+'`rttov`'+rttov_sql)

    crtm_sql = common.get_sql_calc_stat(channel, ins_conf.sql_crtm_bt_lat,
                                        my_table)
    common.debug(my_log, log_tag, 'channel '+str(channel)+'`crtm`'+crtm_sql)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
							 user=conf.db_setting['master']['user'],
							 passwd=conf.db_setting['master']['pwd'], 
							 port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        for one_lat_span in lat_list:
            conn.select_db(conf.table_setting[sat][ins]['data_db'])

            sql = rttov_sql%(one_lat_span*len(my_table) )
            cur.execute(sql)
            '''
            | count(*) | avg(obs_bt1/100 - rttov_bt1/100) | STDDEV_POP
            +----------+----------------------------------+-----------
            |    29612 |                       9.44919999 |9.9472
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
            rttov_data = ('rttov', channel, one_lat_span[1]/factor, ret[0][0],
                          avg, std)
            
            sql = crtm_sql%(one_lat_span*len(my_table) )
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
            crtm_data = ('crtm', channel, one_lat_span[1]/factor, ret[0][0],
                          avg, std)
            
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
    if just_draw:
        print 'just_draw'
        return draw_one_channel(channel)
    
    if just_calc:
        print 'just_calc'
        return calc_one_channel(channel)

    # calc and draw.    
    if not calc_one_channel(channel):
        print 'calc_one_channel'
        return False
    else:
        print 'draw_one_channel'
        return draw_one_channel(channel)
    
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
    l1b_table = [ x for x in all_tables if nwp.upper() in x[0] ]
    for idx, one_table in enumerate(l1b_table):
        table_t = one_table[0][19:32]
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
        print 'FAILED no table found '
        common.err(my_log, log_tag, time_tag + 'FAILED`no table found')
        sys.exit(4)

   # for channel in range(1,ins_conf.channels+1):
   #     print channel
   #     calc_draw_one_channel(channel)
   # return

    pool = Pool(3)
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

