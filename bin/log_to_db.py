#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  Copyright (c) 2014, shinetek.
  All rights reserved.
  
  file name        log_to_db.py
  
  description      all day run program.
  work flow:
    for everyday lofile information, insert to databases
       
       
  how to run manually:
	python log_to_db.py 
                       
  date         author      create
  2014-01-10   zhaowl      create.
"""
__auther__ = "zhaowl"

import os
import sys
import time
import signal
import MySQLdb
import commands
import datetime
import string

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')      
conf = __import__('conf')
common = __import__('common')
ins_conf_file = 'conf'
ins_conf = __import__(ins_conf_file)    

Pstime = ''
Fstime = ''

# pid = os.getpid()
# fname = os.path.splitext(os.path.basename(os.path.realpath(__file__) ) )[0]
# log_tag = fname + '.' + sat + '.' + ins + '.' + str(pid)
# my_name = common.get_local_hostname()
# my_tag = my_name+'.'+log_tag
# my_pidfile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat + '.' \
#             + ins + '.pid'
# my_alivefile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat + '.' \
#             + ins + '.alive'
# my_log = conf.log_path + '/' + my_name + '.' # just prefix: /log/path/prefix.

my_pos_file = conf.pid_path + '/' + my_name + '.' + fname + '.' + 'pos'

def GetTime(strtime):
	tt = ''
    	for c in strtime:
        	if (c >= '0' and c <= '9'):
            		if(tt == ''):
                		tt = c
            		else:
                		tt+=c
        	else:
            		continue 
    	return tt

def LogData_To_Db(table ,insertsql ,cflag):
	#connect database
    	try:
        	conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                                     user=conf.db_setting['master']['user'],
                                     passwd=conf.db_setting['master']['pwd'], 
                                     port=conf.db_setting['master']['port'])
		#print 'user= ' ,user
		#print 'passwd= ' ,passwd
		#print 'port= ' ,port
        	cur=conn.cursor()
        	#conn.select_db(ins_conf.'INFO')
        	conn.select_db(conf.db_setting['info_db'])
    	except MySQLdb.Error, e:
            print "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
            msg = '`Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1] 
            common.err(my_log, log_tag, msg)
            if cmp(str(e.args[0]),'2006') == 0  or cmp(str(e.args[0]),'2013') == 0 :
                conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                                 		user=conf.db_setting['master']['user'],
                                 		passwd=conf.db_setting['master']['pwd'], 
                                        port=conf.db_setting['master']['port'])
                cur=conn.cursor()
            	#conn.select_db(ins_conf.data_db)
        	conn.select_db(conf.db_setting['info_db'])

		#dropsql = 'drop table if exists ' + table
    		#cur.execute(dropsql)
    		#print dropsql
    
        print 'cflag = ',cflag
    	#create table
        if (cflag == 1):
            sql = "create table IF NOT EXISTS %s(`id` int(10) unsigned NOT NULL AUTO_INCREMENT primary key, \
			    date datetime,sat varchar(10),ins varchar(10),pname varchar(50), st datetime, et datetime, sta varchar(20),pid int,info varchar(256))" % (table)
            cur.execute(sql)
            print 'sql: ',sql
            print 'table1 :',table  
            
            addindexsql = 'alter table %s add index(sat)' % (table)
            cur.execute(addindexsql)
            addindexsql = 'alter table %s add index(ins)' % (table)
            cur.execute(addindexsql)
            addindexsql = 'alter table %s add index(pid)' % (table)
            cur.execute(addindexsql)            
    	elif(cflag == 2):
            sql = "create table IF NOT EXISTS %s(`id` int(10) unsigned NOT NULL AUTO_INCREMENT primary key , \
			    fname varchar(256),sat varchar(10),ins varchar(10),pname varchar(50), st datetime, et datetime, sta varchar(20),\
			    utime varchar(10) ,info varchar(256),pid int)"  % (table)
            cur.execute(sql)
            
            #add index for table
            print 'sql: ',sql
            print '@@@table2 :',table  
#             addindexsql = 'alter table %s add index(fname)' % (table)
#             cur.execute(addindexsql)
            addindexsql = 'alter table %s add index(sat)' % (table)
            cur.execute(addindexsql)
            addindexsql = 'alter table %s add index(ins)' % (table)
            cur.execute(addindexsql)    
        elif(cflag == 3):
            sql = "create table IF NOT EXISTS %s(`id` int(10) unsigned NOT NULL AUTO_INCREMENT primary key , \
            		date datetime,sat varchar(10),ins varchar(10),pname varchar(128), sti datetime, eti datetime, ut varchar(20), \
			        sta varchar(20), info varchar(256),pid int)" % (table)           
            cur.execute(sql)
            addindexsql = 'alter table %s add index(pname)' % (table)
            cur.execute(addindexsql)
            addindexsql = 'alter table %s add index(sat)' % (table)
            cur.execute(addindexsql)
            addindexsql = 'alter table %s add index(ins)' % (table)
            cur.execute(addindexsql)    
    	#create sql insert satement
	print '+++++++++sql:::: ',sql
	print 'insertsql: ', insertsql
    	try:
		print('======================================================im here')
        	cur.execute(insertsql)
		
    	except MySQLdb.Error, e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            msg = '`Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
            common.err(my_log, log_tag, msg)
            print insertsql   
	conn.commit()
 	cur.close()
 	conn.close()
def do_crontab(line, proname, pid, tabledic):
    if (line.find('INFO') >0 and line.find('program start') > 0):                        
        strtime = (line.split('`', 5))[0]
        st = GetTime(strtime)
        st = datetime.datetime.strptime(st, "%Y%m%d%H%M%S")
            
        sti = line.split('`', 5)[3]
        sti = GetTime(sti)
        eti = line.split('`', 5)[4]
        eti = GetTime(eti)
        sti = datetime.datetime.strptime(sti, "%Y%m%d%H%M%S")
        eti = datetime.datetime.strptime(eti, "%Y%m%d%H%M%S")
        print 'sti= ', sti
        print 'eti= ', eti

        sat = proname.split('.', 3)[1]
        ins = proname.split('.', 3)[2]
        proname = proname.split('.', 3)[0]
        
        print 'sat= ', sat
        print 'ins= ', ins
        print 'proname= ', proname
        print 'pid= ', pid
                
        #cron program to 'CRONSTA_20140429' table
        insertsql = "insert into %s(date, sat, ins, pname, sti, eti, ut, sta, info, pid)" % (tabledic['table3']) + \
                        "values('%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %d)" % \
                        	(datetime.datetime.strptime(GetTime(strtime)[0:8],"%Y%m%d").date(), sat, ins, proname, sti, eti, \
				                'NULL','NULL', 'NULL', string.atoi(pid))
                        
        LogData_To_Db(tabledic['table3'], insertsql, 3)
        
        #crontab program start time and end time to 'PROSTA' table        
        insertsql = 'insert into %s(date, sat, ins, pname, st, et, sta,pid)' % (tabledic['table1']) + "values('%s','%s', '%s', '%s', '%s', '%s', \
                        '%s', %d)" % (datetime.datetime.strptime(GetTime(strtime)[0:8],"%Y%m%d").date(),sat, ins, proname, st, st,'NULL', \
                            string.atoi(pid))
                        
        LogData_To_Db(tabledic['table1'], insertsql, 1)            
    elif(line.find('INFO') > 0 and line.find('program finish') > 0 and line.find('SUCC') > 0):
        strtime = (line.split('`', 8))[0]
        et = GetTime(strtime)
        et = datetime.datetime.strptime(et, "%Y%m%d%H%M%S")        
        usetime = (line.split('`', 8))[7]
        usetime = usetime.split('=', 2)[-1]
          
        #update to 'CRONSTA_20140429' table 
        updatesql = "update %s set ut = '%s',sta = '%s' where pid = %d" \
                        % (tabledic['table3'], usetime,'ok', string.atoi(pid)) 
                           
        LogData_To_Db(tabledic['table3'], updatesql, 3)
        
        #update to 'PROSTA'
        updatesql = "update %s set et='%s', sta='%s' where pid = %d" \
            % (tabledic['table1'], et, "ok", string.atoi(pid))
        LogData_To_Db(tabledic['table1'], updatesql, 1)
            
    elif(line.find('ERROR') > 0 and line.find('program finish') >0 and line.find('FAILED') > 0):
        strtime = (line.split('`', 8))[0]
        et = GetTime(strtime)
        et = datetime.datetime.strptime(et, "%Y%m%d%H%M%S")        
        usetime = (line.split('`', 8))[7]
        usetime = usetime.split('=', 2)[-1]
        errinfo = '11111'
        
        #update to 'CRONSTA_20140429' table  
        updatesql = "update %s set et = '%s', ut = '%s',sta = '%s',info = '%s' where pid = %d" \
                           	% (tabledic['table3'] ,et, usetime,'error', errinfo, string.atoi(pid)) 
                           
        LogData_To_Db(tabledic['table3'], updatesql, 3)
		
        #update to 'PROSTA'
        updatesql = "update %s set et='%s', sta='%s', info='%s' where pid = %d" \
                        % (tabledic['table1'], et, "error", errinfo, string.atoi(pid))
        LogData_To_Db(tabledic['table1'], updatesql, 1)
        
#def do_hdf_pro(line, proname, pid, tabledic):
              
def do_hdf(line, proname, pid, tabledic):
    
    ##############################################do hdf pro###################################################
    if (line.find('INFO') >0 and line.find('program start') > 0):          
        print "+++++++++++++++++++++++++++++="
        strtime = line[1:20]
        st = ''
        et = 'NULL'
        program_name = (line.split('`', 5))[2]
      
        pid = (line.split('=', 5))[-1]

        print 'pid = ', pid
        st = GetTime(strtime)
        st = datetime.datetime.strptime(st, "%Y%m%d%H%M%S")
        global Pstime 
        Pstime = st
        #et = datetime.datetime.strptime(et, "%Y%m%d%H%M%S")
        sat = program_name.split('.', 3)[1]
        ins = program_name.split('.', 3)[2]
        program_name = program_name.split('.', 3)[0]
        
        insertsql = 'insert into %s(date,sat, ins, pname, st, et, sta, pid)' % (tabledic['table1']) + "values('%s', '%s', '%s', '%s', '%s', '%s', \
        '%s', %d)" % (datetime.datetime.strptime(GetTime(strtime)[0:8],"%Y%m%d").date(), sat, ins, program_name, st, st,'NULL', \
        string.atoi(pid))
        print "=============1==============="
        print 'st= ',st
        print 'program_name= ',program_name
        print 'sql= ',sql
        print '============99999999=========== ',Pstime
        LogData_To_Db(tabledic['table1'], insertsql, 1)  
    elif(line.find('exit now') >0 and line.find('FAILED') > 0):
        strtime = line[1:20]
        program_name = (line.split('`', 5))[2]
        et = ''
        et = GetTime(strtime)
        et = datetime.datetime.strptime(et, "%Y%m%d%H%M%S")

        pid = (line.split('=', 5))[-1]        
        errorinfo = '1111'

        updatesql = "update %s set et='%s', sta='%s', info='%s' where pname = '%s' and pid = %d" \
            % (tabledic['table1'], et, "error", errorinfo, program_name,string.atoi(pid))
        print "=============2==============="
        print 'et ',et
        print 'program_name ',program_name
        print 'sql ',sql
        LogData_To_Db(tabledic.get('table1'), updatesql, 1)    
    
    ##############################################do hdf file###################################################    
    if(line.find("INFO") >0 and (line.find("loading to db now") >0 or line.find("moving now") >0)):
        print "++++++++++++33+++++++++++++++++="
        strtime = (line.split('`', 5))[0]
        st = ''
        st = GetTime(strtime)
        st = datetime.datetime.strptime(st, "%Y%m%d%H%M%S")
        global Fstime 
        Fstime = st
        
        program = (line.split('`', 5))[2]
        allpathfilename = (line.split('`', 5))[3]
        allpathtablename = (allpathfilename.split('/', 6))
        hdffilename = allpathtablename[-1] #get hdf file name
        sat = program.split('.', 3)[1]
        ins = program.split('.', 3)[2]        
        program = program.split('.', 3)[0]
        
        insertsql = 'insert into %s(fname, sat, ins, pname, st, et, sta, utime, info, pid)' % (tabledic['table2']) \
                    + "values('%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s',%d)" % (hdffilename, sat, ins, program, st, st, "NULL", "NULL", "NULL",string.atoi(pid))
        print "=============3==============="
        print 'st= ',st
        print 'program= ',program
        print 'sql= ',insertsql
        LogData_To_Db(tabledic['table2'], insertsql, 2)
    elif((line.find("INFO") >0 and line.find("SUCC") > 0) and (line.find("load obc to db finished") >0 or line.find("move finished") >0)):
        print "++++++++++++44+++++++++++++++++="
        strtime = (line.split('`', 6))[0]
        et = ''
        et = GetTime(strtime)
        et = datetime.datetime.strptime(et, "%Y%m%d%H%M%S")        

        program = (line.split('`', 6))[2]
        allpathfilename = (line.split('`', 6))[3]
        allpathtablename = (allpathfilename.split('/', 6))
        hdffilename = allpathtablename[-1] #get hdf file name
        usetime = (line.split('`', 6))[-1]
        usetime = usetime.split('=')
        
        program = program.split('.',4)[0]
        
        updatesql = "update %s set et = '%s', utime = '%s',sta = '%s' where fname = '%s' and pname = '%s' and pid=%d" \
            % (tabledic['table2'] ,et, usetime[-1],'ok' ,hdffilename, program, string.atoi(pid))
        print "=============4==============="
        print 'et= ',et
        print 'program= ',program
        print 'sql= ',updatesql
        LogData_To_Db(tabledic['table2'], updatesql, 2)
    elif(line.find("ERROR") > 0 and line.find("FAILED") > 0):
        strtime = (line.split('`', 6))[0]
        et = ''
        et = GetTime(strtime)
        et = datetime.datetime.strptime(et, "%Y%m%d%H%M%S")
        
        program = (line.split('`', 6))[2]
        allpathfilename = (line.split('`', 6))[3]
        allpathtablename = (allpathfilename.split('/', 6))
        hdffilename = allpathtablename[-1] #get hdf file name
        usetime = (line.split('`', 6))[-1]
        usetime = usetime.split('=')
        
        errinfo = line.split('`', 7)[5]
        program = program.split('.',4)[0]
        
        updatesql = "update %s set et = '%s', utime = '%s',sta= '%s', errinfo='%s' where fname = '%s' and pname = '%s' and pid=%d" \
                        % (tabledic['table2'] ,et, usetime[-1], 'error', errinfo, hdffilename, program, string.atoi(pid))
        print "=============4==============="
        print 'et= ',et
        print 'program= ',program
        print 'sql= ',updatesql
        LogData_To_Db(tabledic['table2'], updatesql, 2)
    else:
        print('do nothing!')
#         msg = "`log fmt is wrong. please check log fmt log=" + line
#         common.err(my_log, log_tag, msg)  
###################AnalysisString function start#####################                  
def AnalysisString(line, tabledic):
    
    strs = line.split('`', 5)[2]
    proname = strs.split('.', 4)[0] + '.' + strs.split('.', 4)[1] + '.' + strs.split('.', 4)[2] 
    pid = strs.split('.', 4)[3]
    ifproname = strs.split('.', 4)[0]	

    print 'pid = ',pid
    print 'proname = ',proname
	
    if ifproname in conf.do_cron:        
        if ((line.find('ERROR') >0 or line.find('INFO') >0)):
            print 'do_start'
            do_crontab(line, proname, pid, tabledic)
    elif ifproname in conf.do_hdf:
        if (line.find('INFO') >0 or line.find('ERROR') > 0):
            print 'do_hdf'
            do_hdf(line, proname, pid, tabledic)   
    
#####################AnalysisString funtion end#####################
def main():
    
    rflag = 0
    print ("here to start!")
    my_pos_file = conf.pid_path + '/' + my_name + '.' + fname + '-' + time.strftime('%Y%m%d',time.localtime(time.time())) + '.' + 'pos'
    
    if (os.path.exists(my_pos_file) == True): 
        loc = common.rd_file(my_pos_file)
    else:
        common.wt_file(my_pos_file, '')           
        recordseek = 0
    recordtime = time.strftime('%Y%m%d',time.localtime(time.time()))
    while True:    
        CurrDayFileName = conf.data_path +'/LOG/' + common.get_local_hostname() + '.'+ \
					time.strftime('%Y%m%d',time.localtime(time.time())) + '.log'
    	print("CurrDayFileName: %s" % CurrDayFileName)
        tabledic = {'table1':'PROSTA',\
                        'table2':'L1' + 'STATUS_' + time.strftime('%Y%m%d',time.localtime(time.time())),\
                        'table3':'CRONSTA_' + time.strftime('%Y%m%d',time.localtime(time.time()))}  
    	file = open(CurrDayFileName, 'r')
        file.seek(recordseek)
    	for line in file.readlines():      
          
            common.wt_file(my_pos_file, recordseek)  
            line=line.strip('\n')
            if (line.find('daemon') > 0):
                continue
            if (line.find('DEBUG') > 0):
                continue
            
            print line
            AnalysisString(line,tabledic)
            
            if (line.find('INFO') > 0 or line.find('ERROR') > 0):
                time.sleep(1)
#             print('sleepping........................................')
        print('sleepping 10 seconds........................................')
        time.sleep(10)
        recordseek = file.tell()
        file.close()
        
        if (rflag == 1):
            rflag = 0
            my_pos_file = conf.pid_path + '/' + my_name + '.' + fname + '_' + time.strftime('%Y%m%d',time.localtime(time.time())) + '.' + 'pos'
            if (os.path.exists(my_pos_file) == True): 
                data = common.rd_file(my_pos_file)
            else:
                value = 0
                common.wt_file(my_pos_file, value)
            
        if (cmp(recordtime, time.strftime('%Y%m%d',time.localtime(time.time()))) != 0):        
            my_pos_file = conf.pid_path + '/' + my_name + '.' + fname + '_' + time.strftime('%Y%m%d',time.localtime(time.time())) + '.' + 'pos'
            last = datetime.date(datetime.date.today().year,datetime.date.today().month,datetime.date.today().day-1)
            strt = str(last).split('-',3)
            recordtm=strt[0] + strt[1] + strt[2]
            
            yestoday_pos_file = conf.pid_path + '/' + my_name + '.' + fname + '_' + recordtm
            my_pos_file = yestoday_pos_file 
            
            if (os.path.exists(my_pos_file) == True): 
                data = common.rd_file(my_pos_file)
                rflag = 1
            else:
                value = 0
                common.wt_file(my_pos, value)
            recordseek = 0

if __name__ == '__main__':
    main()
