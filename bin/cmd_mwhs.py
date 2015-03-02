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

for i in xrange(1,2):
    cmd1 = '/home/fymonitor/python27/bin/python calc_draw_bt.py --sat=fy3c --ins=mwhs --nwp=t639 --span=6 --date=2014-08-'+str('%02d'%(i))+'-01' + ' --just_calc=true --just_draw=false --save_hdf=false'
    cmd2 = '/home/fymonitor/python27/bin/python calc_draw_bt.py --sat=fy3c --ins=mwhs --nwp=t639 --span=6 --date=2014-08-'+str('%02d'%(i))+'-07' + ' --just_calc=true --just_draw=false --save_hdf=false'
    cmd3 = '/home/fymonitor/python27/bin/python calc_draw_bt.py --sat=fy3c --ins=mwhs --nwp=t639 --span=6 --date=2014-08-'+str('%02d'%(i))+'-13' + ' --just_calc=true --just_draw=false --save_hdf=false'
    cmd4 = '/home/fymonitor/python27/bin/python calc_draw_bt.py --sat=fy3c --ins=mwhs --nwp=t639 --span=6 --date=2014-08-'+str('%02d'%(i))+'-19' + ' --just_calc=true --just_draw=false --save_hdf=false'
    print cmd1
    os.system( cmd1 )
    time.sleep(1)
    print cmd2
    os.system( cmd2 )
    time.sleep(1)
    print cmd3
    os.system( cmd3 )
    time.sleep(1)
    print cmd4
    os.system( cmd4 )
    time.sleep(1)

