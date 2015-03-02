#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:
    my_program --prog=myprog --sat=mysat --ins=myins
    my_program --restart=myrestart --prog=myprog --sat=mysat --ins=myins
    
    my_program --prog=myprog --sat=mysat --ins=myins --nwp=mynwp
    my_program --restart=myrestart --prog=myprog --sat=mysat --ins=myins --nwp=mynwp

Arguments:
    prog  the program name
    sat   the satellite name
    ins   the Instrument name
    nwp   like t639
    restart  restart the program 
"""

__author__ = 'wangzq'

"""All day run program.

This program is the monitor program ,in order to watch the program whether 
alive , if the program gone away ,this monitor program will restart the program
again.

attention: The --restart=restart must put the first place!!!!!!!!!!!!!

like: python daemon.py --restart=restart --prog=sim.py --sat=fy3c --ins=mwts --nwp=t639

"""

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
#                         
# date         author    changes
# 2014-08-14   gumeng    change " if len(pid_list)<30" for obc_to_db threadpool
# 2014-04-21   wangzq    change(add nwp)
# 2014-03-27   wangzq    change to V2
# 2014-03-01   wangzq    change 
# 2014-01-24   gumeng    create.

import time
import signal
import sys
import os
import re
import commands
import string
from dateutil import tz
from datetime import datetime

#import config file path
sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf') 
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

arguments = docopt.docopt(__doc__)
target_program = arguments['--prog']
restart_flag = '' 
kill_parm = '-kill'

target_name = common.get_local_hostname() 
target_tag = target_name + '.' + target_program[0:-3]
cur_time=common.utc_nowdate()
my_log = conf.log_path + '/' + target_name + '.'
log_tag = 'daemon.' + target_program;

if arguments['--sat'] != None and arguments['--ins'] != None and arguments['--nwp'] != None :
    target_pidfile = conf.pid_path + '/' + target_tag + '.' + arguments['--sat'] \
        + '.' + arguments['--ins'] + '.pid'
    target_alivefile = conf.pid_path + '/' + target_tag + '.' + arguments['--sat'] \
        + '.' + arguments['--ins'] + '.alive'   
elif arguments['--sat'] != None and arguments['--ins'] != None and arguments['--nwp'] == None:
    target_pidfile = conf.pid_path + '/' + target_tag + '.' + arguments['--sat'] \
        + '.' + arguments['--ins'] + '.pid'
    target_alivefile = conf.pid_path + '/' + target_tag + '.' + arguments['--sat'] \
        + '.' + arguments['--ins'] + '.alive' 
else:
    target_pidfile = conf.pid_path + '/' + target_tag + '.pid'
    target_alivefile = conf.pid_path + '/' + target_tag + '.alive'

cmd_str=''
if arguments['--restart']:
    restart_flag = arguments['--restart'].upper()
    time.sleep(2)
    for i in range(1,len(sys.argv)):
        cmd_str = cmd_str + sys.argv[i] + ' '
else:
    for i in range(1,len(sys.argv)):
        cmd_str = cmd_str + sys.argv[i] + ' '
#print cmd_str


#define system alarm deal function
def alarm_signal_handler(signum,frame):
    msg = 'after 30 sec, daemon.py recv alarm signal ' + str(signum) + '. exit now.'
    common.warn(my_log, log_tag, msg)
    sys.exit(0)  

#define restart function  
def restart(kill_parm):
    common.debug(my_log, log_tag, cmd_str) 
    #cmd_list=re.split(' ',cmd_str[25:].replace('--',''))
    cmd_list=re.split(' ',cmd_str[:].replace('--',''))
    cmd = ''
    for i in range(1,len(cmd_list)-1):
        cmd = cmd + conf.grep + ' ' + cmd_list[i] + ' | ' 
    common.debug(my_log, log_tag, cmd) 
    
    cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + target_program + ' | ' + cmd \
            + conf.grep + ' -v grep | ' + conf.grep + \
            ' -v daemon.py | ' + conf.grep + ' -v tail | ' + conf.awk +  \
            ' \'{ print $4 }\' ' + ' | ' + conf.xargs + ' -I {} ' + conf.kill \
            + ' ' + kill_parm + ' {}' 
    print cmd
         
    os.system(cmd) #kill all daemon program
    common.err(my_log, log_tag, cmd)       
       
    cmd = conf.rm + ' -rf '+ target_pidfile + ' 2>/dev/null'
    os.system(cmd) #rm pid file
    common.err(my_log, log_tag, cmd)
    
    cmd = conf.rm + ' -rf '+ target_alivefile + ' 2>/dev/null'
    os.system(cmd) #rm alive file
    common.err(my_log, log_tag, cmd)
    
    time.sleep(1)
   
    cmd=''
    cmd_list=re.split(' ',cmd_str)
    if arguments['--restart'] != None:
        for i in range(2,len(cmd_list)-1):
            cmd = cmd + cmd_list[i] + ' '
    else:
        for i in range(1,len(cmd_list)-1):
            cmd = cmd + cmd_list[i] + ' '
    cmd = conf.python + ' ' + conf.bin_path + '/' + target_program + ' '\
            + cmd + ' >> ' + conf.log_path +'/' + target_program +'.log 2>&1 &'
    print cmd          
    os.system(cmd) #restart program
    common.debug(my_log, log_tag, cmd)


def initfunction():
    common.debug(my_log, log_tag, 'Start to do initfunction function!!!')
    #signal.alarm(30)
    signal.signal(signal.SIGALRM,alarm_signal_handler)
    # first, we should kill all of cmd if ps.WCHAN=sync_p.
    cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + conf.bin_path + ' | ' + \
        conf.grep + ' -v grep | ' + conf.grep + ' -v tail | ' + conf.grep + \
        ' sync_p | ' + conf.wc + ' -l'
    (status, total_cnt) = commands.getstatusoutput(cmd)
    common.debug(my_log, log_tag,  cmd + ' Result is find sync_p is: ' + total_cnt + ' Status is: '+ str(status) )
    
    total_cnt=string.atoi(total_cnt) # turn string to digital  

    #kill all ps.WCHAN=sync_p
    if total_cnt > 0 :
        cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + conf.bin_path + ' | ' \
            + conf.grep + ' -v grep | ' + conf.grep + ' -v tail | ' +  \
            conf.grep + ' sync_p | ' + conf.awk + " '{print $4}' | " + \
            conf.xargs + " -I {} " + conf.kill + ' -kill {}' 
        log_str = ': i found ps.WCHAN=sync_p.cnt= ' + total_cnt + \
                ' kill them with: ' + cmd
        common.err(my_log, log_tag, log_str)
        exec_ret_arr = os.system(cmd) 
    
    #/bin/ps -elf | /bin/grep hdf_sim_to_db.py | /bin/grep  '\-\-sat=fy3c' | 
    #/bin/grep  '\-\-ins=mwts' | /bin/grep  '\-\-nwp=t639' | /bin/grep -v grep 
    #| /bin/grep -v daemon.py | /bin/grep -v tail | /bin/grep -v restart | /usr/bin/wc -l
    cmd=''
    cmd_list=re.split(' ',cmd_str.replace('--','\-\-'))
    if arguments['--restart'] != None:
        for i in range(2,len(cmd_list)-1):
            cmd = cmd + conf.grep +  ' \'' + cmd_list[i] +  '\' ' + ' | '
    else:
        for i in range(1,len(cmd_list)-1):
            cmd = cmd + conf.grep +  ' \'' + cmd_list[i] +  '\' ' + ' | '
    print cmd     
    #/bin/grep -v --sat=fy3c | /bin/grep -v --ins=mwts | /bin/grep -v --nwp=t639 | 
    
    #/bin/ps -elf | /bin/grep hdf_sim_to_db.py | /bin/grep -v grep | 
    #/bin/grep -v daemon.py | /bin/grep -v tail | /bin/grep -v restart | /usr/bin/wc -l
    cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + target_program + ' | '  + \
            cmd + conf.grep +  ' -v grep | ' + conf.grep + ' -v daemon.py | ' + \
            conf.grep + ' -v tail | ' + conf.grep +' -v restart | '+ conf.wc + ' -l'
    print cmd
    (status, total_cnt) = commands.getstatusoutput(cmd)
    common.debug(my_log, log_tag, cmd +' Result is: ' + total_cnt + ' Status is: '+ str(status) )
    total_cnt=string.atoi(total_cnt)
    
    if total_cnt > 30 :
        cmd = conf.ps + ' -elf | ' + conf.grep + ' ' + target_program + ' | '\
            + conf.grep + ' -v grep | ' + conf.grep + ' -v restart | ' + \
             conf.grep + ' -v tail | ' + conf.awk + " '{print $4}' | " + \
             conf.xargs + " -I {} " + conf.kill + ' -kill {}'
        common.err(my_log, log_tag, cmd)
        exec_ret_arr = os.system(cmd)
        common.err(my_log, log_tag,' Result is : '+ str(exec_ret_arr))

def main():

    initfunction()

    cmd = conf.cat + ' ' + target_pidfile + ' 2>/dev/null'
    (status, target_pid) = commands.getstatusoutput(cmd)
    
    if restart_flag == 'RESTART':
        msg = 'Pragram\'s has receive \'RESTART\' cmd, system will kill ' +     \
            target_name + ': ' + target_program + ' [ ' +target_pid +' ]' \
            + ' and will restart Pragram!'
        common.err(my_log, log_tag, msg)
        restart(kill_parm)
        sys.exit(0)
 
    cmd_list=re.split(' ',cmd_str[7:].replace('--',''))
    cmd = ''
    for i in range(0,len(cmd_list)-1):
        cmd = cmd + conf.grep + ' ' + cmd_list[i] + ' | ' 
        
    cmd = conf.ps + ' -elf | ' + cmd + conf.grep + ' -v grep | ' + conf.grep \
            + ' -v daemon.py | ' + conf.grep + ' -v tail | ' + conf.awk + ' \' {print $4} \''
    #print cmd
     
    pid_list=os.popen(cmd).readlines()
    common.info(my_log, log_tag, cmd + ' The result is: ' + str(pid_list) )

    print pid_list
    print len(pid_list)
    
    if len(pid_list)<30:
        common.debug(my_log, log_tag, 'find the process the first!!!')
        cmd = conf.cat + ' ' + target_alivefile +' 2>/dev/null '
        print cmd
        (status, last_alive_time) = commands.getstatusoutput(cmd)
        try:
            alive_time_num=time.mktime(time.strptime(last_alive_time,'%Y-%m-%d %H:%M:%S'))
        except:
            alive_time_num = 0
        
        cur_time=common.utc_nowtime()
        sys_time_num=time.mktime(time.strptime(cur_time,'%Y-%m-%d %H:%M:%S'))
        print abs(sys_time_num-alive_time_num)
        
        if abs(sys_time_num-alive_time_num) < conf.default_sleep_time:
            msg = target_name + ': ' + target_program + ' [ ' + target_pid + \
                ' ]' +' is alive at ' + last_alive_time
            common.info(my_log, log_tag, msg)
            sys.exit(0)
        else:
            time.sleep(1)
            try:
                cmd = conf.cat + ' ' +target_alivefile + ' 2>/dev/null'
                (status, last_alive_time) = commands.getstatusoutput(cmd)
                alive_time_num=time.mktime(time.strptime(last_alive_time,'%Y-%m-%d %H:%M:%S'))
                cur_time=common.utc_nowtime()
                sys_time_num=time.mktime(time.strptime(cur_time,'%Y-%m-%d %H:%M:%S'))
            except:
                alive_time_num = 0
                   
            if abs(sys_time_num-alive_time_num) < conf.default_sleep_time:
                msg = ': read alive file again, and find ' + target_name + ': '\
                     + target_program + ' [ ' + target_pid + ' ]  is alive at ' \
                     + last_alive_time
                common.info(my_log, log_tag, msg)
                sys.exit(0)
            else:
                msg = target_name + ': ' + target_program + '[ ' + target_pid \
                    + ' ] will be killed. The alive time is out of time. cur_time= ' \
                    + cur_time + ', last_alive_time= ' + last_alive_time
                common.err(my_log, log_tag, msg)
                restart(kill_parm)    
    else:
        common.debug(my_log, log_tag, 'The first search process not find!!!!')
        msg = target_name + ': ' + target_program + ' [ ' + target_pid + \
            ' ] . Cann\'t find the Process pid and Process will be restart '
        common.err(my_log, log_tag, msg)
        restart(kill_parm)

    sys.exit(0)

if __name__ == '__main__':
    main()