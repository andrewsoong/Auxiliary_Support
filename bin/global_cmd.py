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
from datetime import timedelta

for i in xrange(1,32):
    cmd1 = '/home/fymonitor/python27/bin/python cmd_calc_oneday_global_drawmap.py --sat=fy3c --ins=mwri --just_calc=false --date=2014-11-'+str('%02d'%(i))+ ' --noon_flag=AM'
    cmd2 = '/home/fymonitor/python27/bin/python cmd_calc_oneday_global_drawmap.py --sat=fy3c --ins=mwri --just_calc=false --date=2014-11-'+str('%02d'%(i))+ ' --noon_flag=PM'
    print cmd1
    os.system( cmd1 )
    time.sleep(1)
    print cmd2
    os.system( cmd2 )
    time.sleep(1)

