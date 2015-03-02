#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Temporary program, download data from server.

download data from server,the file dolwnload as er jin zhi

Warning:  
"""

__author__ = 'wangzq'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
#  get sorted hdf data from $input_hdf_path with .OK
#  for each hdf file:
#      insert to db, and touch hdf.db.OK at $input_hdf_path
#         
#                         
# date         author    changes
#
# 2014-02-27   wangzq    create.

import os
import sys
import time
import socket
import ftplib
import datetime

FTPIP="10.24.171.1"
FTPPORT=21
USERNAME="COSFTP"
USERPWD="cosftp"

#localpath = '/hds/assimilation/fymonitor/DATA/GUM/HDF/'
localpath = '/assimilation/fymonitor/DATA/INPUT/HDF/'
now_time = datetime.datetime.now()
yes_time = now_time + datetime.timedelta(days=-3)
ymd = yes_time.strftime('%Y%m%d')
#ymd = '20140807'
# main loop
def download(INS_PATH,ftp_cmd):
    ftp = ftplib.FTP()
    ftp.connect(FTPIP,FTPPORT)
    ftp.login(USERNAME,USERPWD) 
    #print ftp.getwelcome() 
    ftp.cwd(INS_PATH)  
    li = ftp.nlst(ftp_cmd)
    for eachFile in li:
        print eachFile
        bufsize = 1024*1024 #set the buffer size  
        localfile= localpath + eachFile 
        fp = open(localfile,'wb') # open by write module
        ftp.retrbinary('RETR ' + eachFile,fp.write,bufsize) # write file in localtion
        print 'Finish write file ',localfile
        file_ok = localfile + '.OK'
        os.mknod(file_ok) 

    fp.close()
    ftp.quit() 
    
    
def main():
    print 'program start!!!'
    MWTS_PATH = '/D1BDATA/FY3C/MWTS'
    MWTS_CMD = '*GBAL*' + ymd + '*'
    download(MWTS_PATH,MWTS_CMD)
    
    MWHS_PATH = '/D1BDATA/FY3C/MWHS'
    MWHS_CMD = '*GBAL*' + ymd + '*'
    download(MWHS_PATH,MWHS_CMD)
     
    IRAS_PATH = '/D1BDATA/FY3C/IRAS'
    IRAS_CMD = '*GBAL*' + ymd + '*'
    download(IRAS_PATH,IRAS_CMD)
 
    MWRIA_PATH = '/D1BDATA/FY3C/MWRI/MWRIA'
    MWRIA_CMD = '*GBAL*' + ymd + '*'
    download(MWRIA_PATH,MWRIA_CMD)
     
    MWRID_PATH = '/D1BDATA/FY3C/MWRI/MWRID'
    MWRID_CMD = '*GBAL*' + ymd + '*'
    download(MWRID_PATH,MWRID_CMD)
   
    print 'All the file finish download !!'
    
if __name__ == '__main__':
    main()


