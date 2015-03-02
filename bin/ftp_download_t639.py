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
CURRTPATH= "/COMDATA/NWP"
#localpath = '/hds/assimilation/fymonitor/DATA/GUM/T639/'
localpath = '/assimilation/fymonitor/DATA/INPUT/T639/'
now_time = datetime.datetime.now()
yes_time = now_time + datetime.timedelta(days=-2)
ymd = yes_time.strftime('%Y%m%d')

# ymd = now_time.strftime('%Y%m%d')
# print ymd

ftp_cmd = 'gmf.639.' + ymd +'*'
# main loop
def main():
    ftp = ftplib.FTP()
    print 'start main now!!'
    ftp.connect(FTPIP,FTPPORT)
    ftp.login(USERNAME,USERPWD) 
    print ftp.getwelcome() 
    ftp.cwd(CURRTPATH)  
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
    print 'All the file finish download !!'
if __name__ == '__main__':
    main()


