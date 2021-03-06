#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""All day run program, trans gmf.639.2014041712030.grb2 to DAT fmt.
Usage:
    t639_to_bin.py --sat=mysat --ins=myins  
Arguments:
    sat   the satellite name.  like fy3b , fy3c
    ins   the Instrument name. like mwts , mwhs ...
"""

__author__ = 'wangzq'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
#  get sorted grb2 data from input_t639_path with .OK
#  for each grb2 file:
#      check validation.
#      call bin_trans_t639
#      check result, mv to dest path.
#
# Use like: 
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/t639_to_bin.py --sat=fy3c \
# --ins=mwts >> /home/fymonitor/DATA/LOG/t639_to_bin.py.log 2>&1 &
#
# date         author    changes
# 2014-05-15   gumeng    continue change to V2.
# 2014-03-28   wangzq    change to V2
# 2013/12/13   gumeng    create.

import os
import sys
import signal
import time
import commands
sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

# set read-only global variables.
arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].lower()
ins = arguments['--ins'].lower()
print sat
print ins

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
print my_log

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'recv signal ' + str(signum) + '. exit now. pid=' + str(pid)
    common.info(my_log, log_tag, msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

def main():
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, 'program start')
    
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)  
    
    while True:
        # tell daemon that i am alive at current time.
        cur_time = common.utc_nowtime()
        common.wt_file(my_alivefile, cur_time)
        print conf.input_t639_path
        # t639 like gmf.639.2014041900006.grb2
        t639 = common.get_files(conf.input_t639_path, conf.t639,'.OK','t639_name')
        for one_t639 in t639:
            print one_t639
            cur_time = common.utc_nowtime()
            common.wt_file(my_alivefile, cur_time)

            msg = one_t639 + '`trans one t639 now'
            common.info(my_log, log_tag, msg)
            
            time_begin = time.time()            
            filename = os.path.basename(one_t639)
            dest_name = os.path.splitext(filename)[0] + '.DAT'
            file_year = filename[8:12]
            file_mon = filename[12:14]
            cmd_log = conf.tmp_path + '/' + my_tag + '.' + filename + '.' \
                    + common.utc_YmdHMS() + '.log'
            cmd = conf.bash + ' ' + conf.bin_trans_t639 + ' ' + one_t639 + ' '\
                    + conf.output_t639_path + ' > ' + cmd_log + ' 2>&1'
            (status, ret) = commands.getstatusoutput(cmd)
            common.debug(my_log, log_tag, str(status) + '`' + ret + '`' + cmd)
            time_end = time.time()
            timeuse = str(round(time_end - time_begin, 2))
                        
            # check dest DAT file.
            src_file = conf.output_t639_path + '/' + dest_name
            if not common.check_file_exist(src_file, check_ok = False):
                msg = one_t639 + '`FAILED`no output DAT file, redo later ' \
                    + 'automatically.`timeuse=' + timeuse
                common.err(my_log, log_tag, msg)
                continue
            else:
                msg = one_t639 + '`SUCC`trans finished.`timeuse=' + timeuse
                common.info(my_log, log_tag, msg)                
            
            # mv DAT to dest for sim
            # DATA/MNTIN/NWP/T639/2013/10/gmf.639.2013101400000.DAT
            dest_file = conf.sim_input_t639_path + '/' + file_year + '/' \
                      + file_mon + '/' + dest_name
            print dest_file
            common.mv_file(src_file, dest_file)
            os.remove(one_t639 + '.OK')
            os.remove(one_t639)
            os.remove(cmd_log)

        my_sleep_time = common.get_sleep_time()
        for i in range(0, my_sleep_time, 1):
            time.sleep(1)

if __name__ == '__main__':
    main()


