#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, calculate for someday by calling calc_draw_lat.py
finally, create png at end day. Make sure there are data in db for end_t, else
there will NO png at all. 

Usage:
    calc_for_someday_lat.py --sat=fy3c --ins=mwts --nwp=t639  --lat_span=5
    --hour_span=12 --begin_t=2014-01-01 --end_t=2014-01-02

Arguments:
    sat: the satellite you want to calc, fy3c
    ins: the insatrument you want to calc, mwts
    nwp: t639 or ncep
    lat_span: latitude span, 1,2,5,10. ONLY 5 is supported now.
    hour_span: hour span, 12 or 6. ONLY 12 is supported now.
    begin_t: calc for begin date. YYYY-mm-dd like 2014-04-24
    end_t: calc for begin date. YYYY-mm-dd like 2014-04-24
        
"""

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
#    calc y-m-d-h list from begin to end.
#    for each y-m-d-h:
#        calling calc_draw_lat.py
#    for the last y-m-d-h, calling calc_draw_lat.py with draw=True
#
# /home/fymonitor/python27/bin/python
# /home/fymonitor/MONITORFY3C/py2/bin/calc_for_someday_lat.py 
# --sat=fy3c --ins=mwts --nwp=t639 --lat_span=5 --hour_span=12
# --begin_t=2014-01-01 --end_t=2014-01-02
# >> /home/fymonitor/DATA/LOG/calc_for_someday_lat.py.log 2>&1
#                         
# date          author    changes
# 2014-07-28    gumeng    create

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
lat_span = arguments['--lat_span'].lower()
begin_t = arguments['--begin_t'].lower()
end_t = arguments['--end_t'].lower()

pid = os.getpid()
fname = os.path.splitext(os.path.basename(os.path.realpath(__file__) ) )[0]
log_tag = fname + '.' + sat + '.' + ins + '.' + str(pid)
my_name = common.get_local_hostname()
my_tag = my_name+'.'+log_tag
my_log = conf.log_path + '/' + my_name + '.' # just prefix: /log/path/prefix.

common.info(my_log, log_tag, 'program start')

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, msg)
    sys.exit(0)

# create full time span
span_setting = {
    '6': ('00', '06', '12', '18'),
    '12': ('00', '12'),
}

timeArray = time.strptime(begin_t[0:len('2014-07-01')], "%Y-%m-%d")
begin_timestamp = time.mktime(timeArray)
timeArray = time.strptime(end_t[0:len('2014-07-30')], "%Y-%m-%d")
end_timestamp = time.mktime(timeArray)
print begin_timestamp
print end_timestamp
cur_ts = begin_timestamp
last_span = span_setting[hour_span][-1]
while cur_ts <= end_timestamp:
    for one_span in span_setting[hour_span]:
        cur_ymdh = time.strftime("%Y-%m-%d", time.localtime(cur_ts)) + '-' \
                + one_span

        cmd_prefix = 'source ~/.bashrc; ' + conf.python + ' ' + conf.bin_path \
                    + '/calc_draw_lat.py --sat=' + sat + ' --ins=' + ins \
                    + ' --nwp=' + nwp+ ' --hour_span=' + hour_span \
                    + ' --lat_span=' + lat_span \
                    + ' --just_draw=false --save_hdf=false' \
                    + ' --date=' + cur_ymdh
            
        # if cur_ymdh is last_span, we should calc and draw png.
        if cur_ts == end_timestamp and one_span == last_span:
            cmd = cmd_prefix + ' --just_calc=false' 
        else:
            cmd = cmd_prefix + ' --just_calc=true' 

        (status, output) = commands.getstatusoutput(cmd)
        msg = cmd + '`' + str(status) + '`' + output
        print msg
        common.debug(my_log, log_tag, msg)         
    
    cur_ts += 24*60*60

msg = 'SUCC`program finish.`timeuse='
    
timeuse_end = time.time()
timeuse = str(round(timeuse_end - timeuse_begin, 2))
common.info(my_log, log_tag, msg + timeuse)
