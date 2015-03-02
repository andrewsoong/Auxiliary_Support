#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
all day run program

Usage:
   mvfile.py --sat=mysat --ins=myins --nwp=myflag

Arguments:
    sat   the satellite name.  like fy3c
    ins   the Instrument name. like mwts , mwhs,639 ...
    flag  the file flag name.  like gbal,639
    data  [year-mon-day].      must like 2013-12-01
"""

'''
Copyright (c) 2014, shinetek.
All rights reserved.    
	work flow:
	move t639 and hdf file to input dir,then add .ok file
	delete file not meet the standards 
Created on 2014/5/21
@author: zhaowl
'''
__author__ = 'zhaowenlei'

import os
import sys
import time
import signal
import shutil
import datetime
import warnings
import commands

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].upper()
ins = arguments['--ins'].upper()
myflag = arguments['--nwp'].upper()

pid = os.getpid()
fname = os.path.splitext(os.path.basename(os.path.realpath(__file__) ) )[0]
log_tag = fname + '.' + sat.lower() + '.' + ins.lower() + '.' + str(pid)
my_name = common.get_local_hostname()
my_tag = my_name+'.'+log_tag
my_pidfile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat.lower() + '.' \
            + ins.lower() + '.pid'
my_alivefile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat.lower() + '.' \
            + ins.lower() + '.alive'
my_log = conf.log_path + '/' + my_name + '.' # just prefix: /log/path/prefix.

print '===', my_pidfile
print my_alivefile

def signal_handler(signum, frame):
	msg = 'FAILED`recv signal ' + str(signum) + '. exit now.'
	common.info(my_log, log_tag, msg)

	if os.path.isfile(my_pidfile):
		os.remove(my_pidfile)
    
	sys.exit(0)

def put_file(sourcefilename, destfilename):

    print sourcefilename
    print destfilename
    timeuse = 0
    try:
        shutil.move(sourcefilename, destfilename)
    	okfile = destfilename + '.OK'
    	common.wt_file(okfile, '')
#         msg = sourcefilename + '`SUCC`move finished.`timeuse='+str(timeuse)
#         common.info(my_log, log_tag, msg)
    except IOError:   
        msg = sourcefilename + '`FAILED`move file error, the file already exist.`timeuse='+str(timeuse)
        common.err(my_log, log_tag, msg)   
    
def scan_file():
    tt = time.strftime('%Y%m%d',time.localtime(time.time()))
    #for temp use
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-1)
    recordtm = yes_time.strftime('%Y%m%d')
    print recordtm
    sourcedir = conf.ftpfiledir[ins]['source_dir']	
    dest_dir = conf.ftpfiledir[ins]['dest_dir']
    print sourcedir
    print dest_dir 
    for item in os.listdir(sourcedir):
	print item
        savefilename = item
        savefilename = dest_dir + savefilename
        item = sourcedir + item
        if (os.path.isfile(item) == True and \
            os.path.getsize(item) > 0 and \
            item.find(sat) > 0 and item.find(myflag) > 0 \
            and item.find(ins) > 0):
            print 'start !!!!!!!!!!!!'
            msg = item + '`moving now'
            common.info(my_log, log_tag, msg)
            time_begin = time.time() 
            put_file(item, savefilename)
            time_end = time.time()
            timeuse = str(round(time_end - time_begin, 2))		
            msg = item + '`SUCC`move finished.`timeuse='+str(timeuse)
            common.info(my_log, log_tag, msg)
        elif(os.path.isfile(item) != True):
            timeuse = 0
            msg = item + "`FAILED`the file error,is not a file.`timeuse=" + str(timeuse)
            common.err(my_log, log_tag, msg)
            os.remove(item)
            continue
#         elif (item.find(recordtm) < 0):
#             timeuse = 0
#             msg = item+"`FAILED`the file error,file time error,recordtm=" + recordtm +'`timeuse=' + str(timeuse)   		
#             common.err(my_log, log_tag, msg)
#             os.remove(item)
#             continue 
        elif (os.path.getsize(item) < 0):
            timeuse = 0
            msg = item + "`FAILED`the file error,file size is empty,size=" + os.path.getsize(item)+'\
				`timeuse='+istr(timeuse)
            common.err(my_log, log_tag, msg)
            os.remove(item)
            continue
        elif (item.find(sat) < 0):
            timeuse = 0
            msg = item + "`FAILED`the file error,sat flag error,sat=" + sat + '`timeuse=' + str(timeuse)
            common.err(my_log, log_tag, msg)
            os.remove(item)
            continue 
        elif (item.find(myflag) < 0):
            timeuse = 0	
            msg = item +"`FAILED`the file error,myflag flag error,myflag=" + myflag + '`timeuse=' + str(timeuse)
            common.err(my_log, log_tag, msg)
            os.remove(item)
            continue
        elif (item.find(ins) < 0):
            timeuse = 0	
            msg = item + "`FAILED`the file error,ins flag error,ins=" + ins + '`timeuse=' + str(timeuse)
            common.err(my_log, log_tag, msg)
            os.remove(item)
            continue           

def main():
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, 'program start. pid=' + str(pid))
 
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)  
    
    while True:
		cur_time = common.utc_nowtime()		
		common.wt_file(my_alivefile, cur_time)		
               
		scan_file()
		my_sleep_time = common.get_sleep_time()
		for i in range(0, my_sleep_time, 1):
			time.sleep(1)
if __name__ == '__main__':
    main()
