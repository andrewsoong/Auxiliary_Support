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

hdf_source_dir = '/hds/assimilation/fymonitor/DATA/GUM/HDF/'
# hdf_dest_dir = '/home/fymonitor/DATA/INPUT/HDF/'
hdf_dest_dir = {'MWTS':'/hds/assimilation/fymonitor/DATA/FTPDATA/MWTS/',
                'MWHS':'/hds/assimilation/fymonitor/DATA/FTPDATA/MWHS/',
                'MWRI':'/hds/assimilation/fymonitor/DATA/FTPDATA/MWRI/',
                'IRAS':'/hds/assimilation/fymonitor/DATA/FTPDATA/IRAS/',
                'T639':'/hds/assimilation/fymonitor/DATA/FTPDATA/T639/'
                }

def put_file(mvfilename, savedestfilename, tt):
    #print 'file: ', mvfilename 
    saveoriginalfilename = savedestfilename
    savedestfilename = savedestfilename + '.tmp'
    if (os.path.exists(savedestfilename) == False):
        shutil.move(mvfilename, savedestfilename)
        #delete .tmp
        os.rename(savedestfilename, saveoriginalfilename)
        #print 'time:[%s] file :[%s] ' % (tt,mvfilename)

def find_hdf():
    strtm = time.strftime('%Y%m%d%H%M',time.localtime(time.time()))
#     last = datetime.date(datetime.date.today().year,datetime.date.today().month,datetime.date.today().day)
#     strt = str(last).split('-',3)
    
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-1)
    ymd = yes_time.strftime('%Y%m%d')
    
    recordtm=ymd +'_'+strtm[-4:]
    print recordtm
    #print time.asctime(time.struct_time(tm_year=2008, tm_mon=9, tm_mday=19))
    #print time.struct_time(tm_year=strtm[0:4],tm_mon=strtm[4:2],tm_mday=strtm[6:2])
    #print strtm
    for item in os.listdir(hdf_source_dir):
        savefilename = item
        item = hdf_source_dir + item
        if (os.path.isfile(item) == True and item.find(recordtm) > 0 and item.find('.OK') < 0):
        #if (os.path.isfile(item) == True and item.find('20140609_0903') > 0 and item.find('.OK') < 0):
            if (savefilename.find('MWTS') > 0):            
                savedestfilename = hdf_dest_dir['MWTS'] + savefilename
                put_file(item, savedestfilename, strtm)
            elif (savefilename.find('MWHS') > 0):
                savedestfilename = hdf_dest_dir['MWHS'] + savefilename
                put_file(item, savedestfilename, strtm)
            elif (savefilename.find('IRAS') > 0):
                savedestfilename = hdf_dest_dir['IRAS'] + savefilename
                put_file(item, savedestfilename, strtm)
            elif (savefilename.find('MWRI') > 0):
                savedestfilename = hdf_dest_dir['MWRI'] + savefilename
                put_file(item, savedestfilename, strtm)
            elif (savefilename.find('639') > 0):
                savedestfilename = hdf_dest_dir['T639'] + savefilename
                put_file(item, savedestfilename, strtm)
                
        #print os.path.basename(item)
        
def main():
    #print 'here to start!'
    find_hdf()
    
if __name__ == '__main__':
    main()
