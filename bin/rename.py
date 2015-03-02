#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import numpy
import signal
import commands
import warnings
import MySQLdb
import string
import h5py as h5
import shutil
from datetime import datetime
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')

mwts_path = '/home/fymonitor/mysql51/data/FY3C_MWTS_V2/'
mwhs_path = '/home/fymonitor/mysql51/data/FY3C_MWHS_V2/'
mwri_path = '/home/fymonitor/mysql51/data/FY3C_MWRI_V2/'
iras_path = '/home/fymonitor/mysql51/data/FY3C_IRAS_V2/'

def rename(file_path):
    all_file = os.listdir(file_path)
    file_list = []
    for file in all_file:
        if file=='.' or file=='..' or os.path.isdir(file)==True:
            continue
        if os.path.isfile(item) != True:
            continue
        




def main():
    rename(mwts_path)





if __name__ == '__main__':
    main()