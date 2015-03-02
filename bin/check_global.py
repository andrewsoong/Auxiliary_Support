#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, for lat span, calculate bt avg(obs-sim), 
STDDEV_POP(obs-sim), etc. by each 12 or 6 hours. insert to db.
and, draw bt lat png.

Usage:
    calc_draw_lat_by_point.py --sat=fy3c --ins=mwts
"""

__author__ = 'wzq'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
# crontabed every 12 hours, then
#      check ps result, kill previous same program, avoiding hang.
#      get time span, lat span.
#      get calc sql
#      run sql, save calc result to STAT db
#      export all life time series data to hdf
#      draw png
#      mv png to dest path
#         

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
from datetime import datetime
from datetime import timedelta
from multiprocessing import Pool 

warnings.filterwarnings('ignore', category = MySQLdb.Warning)

timeuse_begin = time.time()

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

# set read-only global variables.

arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].lower()
ins = arguments['--ins'].lower()

ins_conf_file = sat.upper() + '_' + ins.upper() + '_CONF'
ins_conf = __import__(ins_conf_file)

if sat not in conf.support_sat_ins or ins not in conf.support_sat_ins[sat]:
    print 'sat or ins setting is NOT found in conf.py'
    sys.exit(0)
    
def main():
    for i in xrange(1,3):
        cmd = 'cd /hds/assimilation/fymonitor/DATA/IMG/NSMC/FY3C/'+ ins.upper() +'/GLOBAL/2014/11/'
        print cmd
        commands.getstatusoutput(cmd)
        cmd = 'ls *201411'+str('%02d'%(i))+'*AM* | wc -l'
        print cmd
        (status, output) = commands.getstatusoutput(cmd)
        print output
        print status
if __name__ == '__main__':
    main()

