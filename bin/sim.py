#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""All day run program, calc simulation bin data for L1B hdf.

For FY3B data, L1B hdf file include: L1B(***KM)
For FY3C data, L1B hdf file include: L1B(***KM), OBC, GEO.

Usage:
    sim.py --sat=mysat --ins=myins --nwp=mynwp

Arguments:
    sat: the satellite you want to calc
    ins: the insatrument you want to calc
    nwp: t639 or ncep

"""

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
#  get sorted hdf data from input_hdf_path with .OK
#  for each hdf file:
#      check validation.
#      mv hdf to arch if 2 days before
#      check t639 data validation
#      call simulation fortran exe for sim
#      check simulation result TOVSL1X.OK, and create SIM.OK
#         
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/sim.py --sat=fy3c 
# --ins=mwts --nwp=t639 >> /home/fymonitor/DATA/LOG/sim.py.log 2>&1
#                         
# date          author    changes
# 2014-05-28    gumeng    check sim.ok in sim() function. return false if fail
# 2014-05-15    gumeng    update to v2.
# 2014-03-27    gumeng    create

import os
import sys
import time
import signal
import commands

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

# set read-only global variables.

arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].lower()
ins = arguments['--ins'].lower()
nwp = arguments['--nwp'].lower()

ins_conf_file = sat.upper() + '_' + ins.upper() + '_CONF'
ins_conf = __import__(ins_conf_file)

if sat not in conf.support_sat_ins or ins not in conf.support_sat_ins[sat] \
    or nwp not in conf.support_nwp:
    print 'sat or ins or nwp setting is NOT found in conf.py'
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

# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

# check hdf filename's time info, mv to ARCH path if days_before
# the only way to calc days_before hdf is redo!
def is_days_before(hdf, days_before):
    return False

# check enough nwp is ready for hdf to calc.
# if no nwp data, just mv hdf to ARCH path for redo.
def have_enough_nwp(hdf):
    return True

def sim(hdf):
    (filename, year, mon) = common.get_filename_year_mon(hdf, ins_conf)
    
    # /home/fymonitor/DATA/SAT/NSMC/FY3C/MWTS/L1/2013/10
    sim_input = conf.sim_original_hdf_path + '/' + sat.upper() + '/' \
                + ins.upper() + '/L1/' + year + '/' + mon + '/'
    sim_file = sim_input + filename
    if not os.path.isfile(sim_file):
        os.symlink(hdf, sim_file)
    
    # /bin/bash /path/of/Monitor_SDR_FileInvoke_NOCMP.bash \
    # /home/fymonitor/DATA/SAT/NSMC/FY3C/MWTS/L1/2014/01/ \
    # FY3C_MWTSX_GBAL_L1_20140101_0658_060KM_MS.HDF > log 2>&1 
    cmd_log = conf.log_path + '/' + my_name + '.' + filename + '.' \
            + common.utc_YmdHMS() + '.log' 
    cmd = 'cd ' + conf.sim_script_path + ' ; ' + conf.bash + ' ' \
        + conf.bin_sim[sat] + ' ' + sim_file + ' > ' + cmd_log + ' 2>&1'
    (status, output) = commands.getstatusoutput(cmd)
    common.debug(my_log, log_tag, str(status) + '`' + cmd + '`' + output)
    
    # check TOVSL1X.OK for rttov and crtm
    if not common.sim_ok(sat, ins, nwp, hdf):
        return False
    else:
        os.remove(sim_file)
        os.remove(cmd_log)    
        return True
    
# main loop
def main():
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, 'program start')
    
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)  
    
    while True:
    	# tell daemon that i am alive at current time.
    	cur_time = common.utc_nowtime()
        common.wt_file(my_alivefile, cur_time)
        
        hdf = common.get_files(conf.input_hdf_path, ins_conf.L1B, '.OK', 'hdf')
        
        for one_hdf in hdf:
            cur_time = common.utc_nowtime()
            common.wt_file(my_alivefile, cur_time)

            # check *SIM.OK file, avoid redo.
            sim_ok_file = one_hdf + '.SIM.OK'
            if os.path.isfile(sim_ok_file) or not os.path.isfile(one_hdf+'.OK'):
                continue

            # whatever l0, l1b, obc, geo files ok or not, we can sim!

            days_before = 2
            if is_days_before(one_hdf, days_before):
                continue
            
            if not have_enough_nwp(one_hdf):
                continue
            
            msg = one_hdf + '`simulating now'
            common.info(my_log, log_tag, msg)
            
            time_begin = time.time()
            ret = sim(one_hdf)
            time_end = time.time()
            timeuse = str(round(time_end - time_begin, 2))
            
            # check TOVSL1X.OK for rttov and crtm, and touch SIM.OK
            if ret:
                common.wt_file(sim_ok_file, sim_ok_file)
                msg = one_hdf + '`SUCC`simulation finished.`timeuse=' + timeuse
                common.info(my_log, log_tag, msg)
            else:
                msg = one_hdf + '`FAILED`simulation finished, but rttov or ' \
                    + 'crtm ok flag file not find, redo later ' \
                    + 'manually.`timeuse=' + timeuse
                common.err(my_log, log_tag, msg)
                common.mv_hdf(one_hdf + '.OK', conf.redo_hdf_path)
                common.mv_hdf(one_hdf, conf.redo_hdf_path)
                
    	my_sleep_time = common.get_sleep_time()
    	for i in range(0, my_sleep_time, 1):
    		time.sleep(1)

if __name__ == '__main__':
    main()


