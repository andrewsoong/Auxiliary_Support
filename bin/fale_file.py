from datetime import timedelta
import numpy
import h5py as h5
import commands
import MySQLdb
import signal
import time
import sys
import os
import threading
import datetime
import shutil
import random

def time_to_arr(data):
    return [data[0:4], data[5:7], data[8:10], \
              data[11:13], data[14:16]]


now_time = datetime.datetime.now()

time14 = ['00-30','02-00','04-15','05-50','06-15','08-50','10-40','11-30','12-20'\
          ,'14-30','16-40','18-30','21-50','23-40']

time15 = ['00-30','02-00','04-15','05-50','06-15','08-50','10-40','11-30','12-20'\
          ,'14-30','16-40','18-30','21-50','22-30','23-40']

#ymd = systime[0:4] + '-' + systime[4:6] + '-' + systime[6:8]

txt_file = '/home/fymonitor/DATA/TEMPHDF/fale_file.txt'
output = open(txt_file, 'w')

for i in range(0, 304):
    yes_time = now_time + datetime.timedelta(days=(-304 + i))
    ymd = yes_time.strftime('%Y-%m-%d')
    print ymd
    temp_num=random.sample('45',1)
    print temp_num
    if temp_num == ['4']:
        for index,val in enumerate(time14):
            content = ymd + '-' + val + ',' + ("%.2f" % random.uniform(0, 4))+'\n' 
            print content 
            output.write(content)
    elif temp_num == ['5']:
        for index,val in enumerate(time15):
            content = ymd + '-' + val + ',' + ("%.2f" % random.uniform(0, 4))+'\n'
            print content
            output.write(content)
output.close()

numpy_data = numpy.loadtxt(txt_file,dtype='str',delimiter=',')

hdfname = '/home/fymonitor/DATA/TEMPHDF/wang.hdf'
hfile = h5.File(hdfname, 'w')

ymdh_arr = numpy.array(map(time_to_arr, numpy_data[:, 0]) )
hfile.create_dataset("time", data = ymdh_arr.astype(numpy.int32))

lat = hfile.create_dataset("Nedt",data=numpy_data[: ,1].astype(numpy.float32))

hfile.close()