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
# 2014-08-29    gumeng    qsub to [node61, node65] with queue.
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

#/home/fymonitor/DATA/INPUT/HDF/FY3C_MWTSX_GBAL_L1_20140430_0641_033KM_MS.HDF
def sim(hdf):
    print hdf
    temppbs_file = '/home/fymonitor/PARAOPER/scripts/qsubs/subfile.pbs'
    (filename, year, mon) = common.get_filename_year_mon(hdf, ins_conf)
    sat = os.path.split(hdf)[1][0:4]
    ins = os.path.split(hdf)[1][5:9]

    # /home/fymonitor/DATA/SAT/NSMC/FY3C/MWTS/L1/2013/10
    sim_input = conf.sim_original_hdf_path + '/' + sat.upper() + '/' \
                + ins.upper() + '/L1/' + year + '/' + mon + '/'
    sim_file = sim_input + filename
    #/home/fymonitor/DATA/SAT/NSMC/FY3C/MWTS/L1/2014/04/FY3C_MWTSX_GBAL_L1_20140430_0641_033KM_MS.HDF
    try:
        if not os.path.isfile(sim_file):
            os.symlink(hdf, sim_file)
    except (OSError, IOError) as e:
        msg = hdf + '`link hdf error[' + str(e.args[0])+']: ' + e.args[1]
        common.error(my_log, log_tag, msg)
        return False            
    
    #bash  /home/fymonitor/PARAOPER/scripts/procs/Monitor_SDR_FileInvoke_nocompile.bash /home/fymonitor/PARAOPER FY3C_MWHS_L1_NSMCHDF_T639_RTTOV101  /home/fymonitor/DATA/SAT/NSMC/FY3C/MWHS/L1/2014/05/FY3C_MWHSX_GBAL_L1_20140530_1912_015KM_MS.HDF
    #bash  /home/fymonitor/PARAOPER/scripts/procs/Monitor_SDR_FileInvoke_nocompile.bash /home/fymonitor/PARAOPER FY3C_MWHS_L1_NSMCHDF_T639_CRTM202  /home/fymonitor/DATA/SAT/NSMC/FY3C/MWHS/L1/2014/05/FY3C_MWHSX_GBAL_L1_20140530_1912_015KM_MS.HDF
    #'FY3C_MWHS_L1_NSMCHDF_T639_RTTOV101'
    mpi_bash = '/home/fymonitor/PARAOPER/scripts/procs/Monitor_SDR_FileInvoke_nocompile.bash '
    
    file_content_101 = 'source ~/.bashrc; bash ' + ' ' + mpi_bash + '/home/fymonitor/PARAOPER'\
                     + '  ' + sat.upper() + '_' + ins.upper() + \
                     '_L1_NSMCHDF_T639_RTTOV101 ' + sim_file
    file_content_202 = 'source ~/.bashrc; bash ' + ' ' + mpi_bash + '/home/fymonitor/PARAOPER' \
                    + '  ' + sat.upper() + '_' + ins.upper() + \
                    '_L1_NSMCHDF_T639_CRTM202 ' + sim_file
                    
    common.wt_file(temppbs_file, file_content_101 + '\n' + file_content_202 \
                   + '\n')
    common.debug(my_log, log_tag, hdf + '`' + file_content_101 + '`' + \
                 file_content_202)
    
    cmd = 'source ~/.bashrc; cd /home/fymonitor/PARAOPER/scripts/qsubs; ' \
        + 'qsub -q fymonitor -l nodes=1:fy:ppn=1 ' + temppbs_file
    (status, output) = commands.getstatusoutput(cmd)
    common.debug(my_log, log_tag, hdf + '`' + str(status) + '`' + cmd + '`' + output)
    
    #qstat | grep 195.sugonadmin1| awk '{print $5}'
    cmd = 'qstat | grep ' + output + ' | awk \'{print $5}\' '
    
    sleep_cnt = 0
    output = 'E'
    while True:
        sleep_cnt += 1
        if sleep_cnt > 10 * 60:
            output = 'E'
            common.err(my_log, log_tag, hdf + '`max then 10*60 sec, timeout.')
            break
        
        (status, output) = commands.getstatusoutput(cmd)
#         common.debug(my_log, log_tag, str(status) + '`' + cmd + '`' + output)
        #print output
        if output is 'C':
            common.debug(my_log, log_tag, hdf + '`sim over.')
            break
        elif output is 'E':
            common.debug(my_log, log_tag, hdf + '`got err status from qstat.')
            time.sleep(1)
        else:
            time.sleep(1)   
    
    if output is 'E':
        return False
        
    if not common.check_sim_bin(sat, ins, nwp, hdf):
        common.error(my_log, log_tag, hdf + '`sim bin data check failed.')
        return False
    else:
#         os.remove(sim_file)
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
        print conf.input_hdf_path
        hdf = common.get_files(conf.input_hdf_path, '^FY3C_\D{5}_GBAL_L1_\d{8}_\d{4}_\d{3}KM_MS\.HDF\.OK$', '.OK', 'hdf')
    
        for one_hdf in hdf:
            cur_time = common.utc_nowtime()
            common.wt_file(my_alivefile, cur_time)
            
            # check *SIM.OK file, avoid redo.
            sim_ok_file = one_hdf + '.SIM.OK'
            print sim_ok_file
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
                msg = one_hdf + '`FAILED`simulation finished, but error ' \
                    + 'occured, redo later ' \
                    + 'manually.`timeuse=' + timeuse
                common.err(my_log, log_tag, msg)
                try:
                    pass
#                     common.mv_hdf(one_hdf + '.OK', conf.redo_hdf_path)
#                     common.mv_hdf(one_hdf, conf.redo_hdf_path)
                except:
                    msg = one_hdf + '`mv file to redo path error.'
                    common.err(my_log, log_tag, msg)
                
    	my_sleep_time = common.get_sleep_time()
    	for i in range(0, my_sleep_time, 1):
    		time.sleep(1)

if __name__ == '__main__':
    main()


