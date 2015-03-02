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


for i in xrange(1,15):
    import os
    pipe = os.popen('ls /hds/assimilation/fymonitor/DATA/IMG/NSMC/FY3C/MWRI/GLOBAL/2014/12/*201412'+str('%02d'%(i))+'*AM* | wc -l', 'r')
    time.sleep(3)
    a = pipe.read()
    b=int(a)
    #p=subprocess.Popen('ls /hds/assimilation/fymonitor/DATA/IMG/NSMC/FY3C/MWTS/GLOBAL/2014/12/*201412'+str('%02d'%(i))+'* | wc -l',stdout=subprocess.PIPE)
    #a=p.stdout.read().decode('utf8')
    print('201412'+str('%02d'%(i)))
    print('AM'+a)
    if(b!=10):
        cmd1 ='/home/fymonitor/python27/bin/python /home/fymonitor/MONITORFY3C/py2/bin/cmd_calc_oneday_global_drawmap.py --sat=fy3c --ins=mwri --just_calc=false --date="2014-12-'+str('%02d'%(i))+'" --noon_flag=AM'
        print cmd1
        os.system( cmd1 )
        time.sleep(2)
    import os
    pipe1 = os.popen('ls /hds/assimilation/fymonitor/DATA/IMG/NSMC/FY3C/MWRI/GLOBAL/2014/12/*201412'+str('%02d'%(i))+'*PM* | wc -l', 'r')
    time.sleep(3)
    a = pipe1.read()
    b=int(a)   
    print('201412'+str('%02d'%(i)))
    print('PM'+a)  
    if(b!=10):
        cmd1 ='/home/fymonitor/python27/bin/python /home/fymonitor/MONITORFY3C/py2/bin/cmd_calc_oneday_global_drawmap.py --sat=fy3c --ins=mwri --just_calc=false --date="2014-12-'+str('%02d'%(i))+'" --noon_flag=PM'
        print cmd1
        os.system( cmd1 )
        time.sleep(2)
        
    

