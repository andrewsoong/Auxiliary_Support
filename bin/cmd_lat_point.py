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

for i in xrange(20,31):
    cmd1 = '/home/fymonitor/python27/bin/python calc_draw_lat_by_point.py --sat=fy3c --ins=mwts --nwp=t639 --lat_span=5 --hour_span=6 --just_calc=true --just_draw=false --save_hdf=false --date=2014-10-'+str('%02d'%(i))+'-01'
    cmd2 = '/home/fymonitor/python27/bin/python calc_draw_lat_by_point.py --sat=fy3c --ins=mwts --nwp=t639 --lat_span=5 --hour_span=6 --just_calc=true --just_draw=false --save_hdf=false --date=2014-10-'+str('%02d'%(i))+'-07'
    cmd3 = '/home/fymonitor/python27/bin/python calc_draw_lat_by_point.py --sat=fy3c --ins=mwts --nwp=t639 --lat_span=5 --hour_span=6 --just_calc=true --just_draw=false --save_hdf=false --date=2014-10-'+str('%02d'%(i))+'-13'
    cmd4 = '/home/fymonitor/python27/bin/python calc_draw_lat_by_point.py --sat=fy3c --ins=mwts --nwp=t639 --lat_span=5 --hour_span=6 --just_calc=true --just_draw=false --save_hdf=false --date=2014-10-'+str('%02d'%(i))+'-19'
    print cmd1
    os.system( cmd1 )
    time.sleep(3)
    print cmd2
    os.system( cmd2 )
    time.sleep(3)
    print cmd3
    os.system( cmd3 )
    time.sleep(3)
    print cmd4
    os.system( cmd4 )
    time.sleep(3)

