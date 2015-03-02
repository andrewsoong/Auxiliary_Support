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
    cmd1 = '/home/fymonitor/python27/bin/python  /home/fymonitor/MONITORFY3C/py2/bin/Data_Out.py  --sat=fy3c --ins=mwhs --span=12 --date=201411'+str('%02d'%(i))
    print cmd1
    os.system( cmd1 )
    time.sleep(1)
    

