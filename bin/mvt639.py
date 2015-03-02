'''
Created on 2014/5/15
work flow:
    move hdf source dir file to hdf dest dir
@author: zhaowenlei
'''
import os
import re
import sys
import signal
import time
import shutil
import datetime
import commands
sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
import conf
sys.path.append('/home/fymonitor/MONITORFY3C/py2/lib')
import common

t639_source_dir = '/hds/assimilation/fymonitor/DATA/GUM/T639/'
# hdf_dest_dir = '/home/fymonitor/DATA/INPUT/HDF/'
t639_dest_dir = '/hds/assimilation/fymonitor/DATA/FTPDATA/T639/'

def put_file(mvfilename, savedestfilename, tt):
    #print 'file: ', mvfilename 
    saveoriginalfilename = savedestfilename
    savedestfilename = savedestfilename + '.tmp'
    if (os.path.exists(savedestfilename) == False):
        shutil.move(mvfilename, savedestfilename)
        #delete .tmp
        os.rename(savedestfilename, saveoriginalfilename)
        #print 'time:[%s] file :[%s] ' % (tt,mvfilename)

def find_hdf(para):
    strtm = time.strftime('%Y%m%d%H%M',time.localtime(time.time()))
    last = datetime.date(datetime.date.today().year,datetime.date.today().month,datetime.date.today().day-1)

    strt = str(last).split('-',3)
    
    recordtm=strt[0] + strt[1] + strt[2] + para
    print recordtm

    for item in os.listdir(t639_source_dir):
        savefilename = item
        item = t639_source_dir + item
        if (os.path.isfile(item) == True and item.find(recordtm) > 0 and item.find('.OK') < 0 \
		and savefilename.find('639') > 0):
		savedestfilename = t639_dest_dir + savefilename
                put_file(item, savedestfilename, strtm)
                
        #print os.path.basename(item)
        
def main():
    if (len(sys.argv) != 2):
	print 'input parameter error!!'
	sys.exit(0)
   
    find_hdf(sys.argv[1])
    
if __name__ == '__main__':
    main()
