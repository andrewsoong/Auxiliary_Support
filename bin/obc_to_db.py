#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""All day run program, put OBC hdf data to db.

Usage:
    obc_to_db.py --sat=mysat --ins=myins

Arguments:
    sat: the satellite you want to calc
    ins: the insatrument you want to calc

"""
#from __builtin__ import True
#from bin.calc_oneday_global import sql

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
#  get sorted hdf obc data from input_hdf_path with .OK
#  for each hdf file:
#      check validation
#      construct mysql table record
#      read hdf, create array of mysql table record.
#      wt array to tmpfile
#      mysql: load data infile 'tmpfile'
#      insert basic info to STAT db
#         
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/obc_to_db.py --sat=fy3c 
# --ins=mwts >> /home/fymonitor/DATA/LOG/obc_to_db.py.log 2>&1
#                         
# date          author    changes
# 2014-08-11    gumeng    add calc_nedn_to_db for mwri, iras. and,  
#                get_scanperiod(), and calc_obc_to_db()
# 2014-08-02    gumeng    add calc_nedn_to_db function.
# 2014-06-13    gumeng    for fy3c.mwri, there are no 3-dims obc table at all.
#                and, mwri ms_cnt is very hard to trans_to_db().
#                and, check ins.conf setting before create table. 
# 2014-04-22    gumeng    for obc 3 dims data, rd then load to db.
# 2014-04-05    gumeng    create

import os
import sys
import time
import numpy
import signal
import commands
import warnings
import MySQLdb
import h5py as h5
from multiprocessing import Pool

warnings.filterwarnings('ignore', category = MySQLdb.Warning)

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
    
pid = os.getpid()    
fname = os.path.splitext(os.path.basename(os.path.realpath(__file__) ) )[0]
log_tag = fname + '.' + sat + '.' + ins + '.' + str(pid)
my_name = common.get_local_hostname()
my_tag = my_name+'.'+log_tag
my_pidfile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat + '.' \
            + ins + '.pid'
my_alivefile = conf.pid_path + '/' + my_name + '.' + fname + '.' + sat + '.' \
            + ins + '.alive'
my_log = conf.log_path + '/' + my_name + '.' # just prefix: /log/path/prefix.
     
# Deal with signal.
def signal_handler(signum, frame):
    msg = 'FAILED`recv signal ' + str(signum) + '. exit now'
    common.info(my_log, log_tag, msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

# get scans, pixels, sds 2 dims data from hdf.
# sds dtype is db_dtype for db.
def get_data_from_hdf(hdf_name, sds_settings):
    
    #gumeng: get *KM hdf from hdf_name
    #if hs, check file exists.
    
    data = {'ok': False, 'scans': 0, 'pixels': 0, 'hdf_data': None,
            'daycnt': None, 'mscnt': None}

    attr = common.get_attr_from_hdf(hdf_name, ins_conf)
    
    if not attr['ok']:
        common.err(my_log, log_tag, attr['msg'])
        return data
    
        
    
    data['scans'] = attr['scans']
    data['pixels'] = attr['pixels']
        
    hdf_fd = {'l0': None, 'l1b': None, 'l1b_obc': None, 'l1b_geo': None}
    hdf_fd['l1b_obc'] = h5.File(hdf_name, 'r')
    if (ins=='mwhs'):
        if not os.path.exists(hdf_name):
            common.err(my_log, log_tag, hdf_name + '`not exist mwhs obcxx file.') 
            print hdf_name + '`not exist mwhs obcxx file.'
            return data
        km_name=hdf_name.replace('OBCXX','015KM')
        if not os.path.exists(km_name):
            common.err(my_log, log_tag, hdf_name + '`not exist mwhs 15km file.') 
            print hdf_name + '`not exist mwhs obcxx file.'
            return data
        # if hs, then open km_name file.
        hdf_fd['l1b'] = h5.File(km_name, 'r')
    
    

    #get all data in HDF
    total_data = []

    sds_len = len(sds_settings)
    for i in xrange(1, sds_len + 1):
        sds_setting = sds_settings[i]
        sds = sds_setting['sds']
        src = sds_setting['src']
        
        if sds not in hdf_fd[src]:
            msg = hdf_name + '`' + src + ' file, sds ' + sds + ' not exist.'
            common.err(my_log, log_tag, msg)
            return data
        
        dataset = hdf_fd[src][sds]
        # h5yp: To retrieve the contents of a scalar dataset, can use the 
        # same syntax as in NumPy: result = dset[()]. In other words, index 
        # into the dataset using an empty tuple.
        sds_data = common.trans_data_for_db(dataset[()], sds_setting)
        total_data.extend([sds_data])
    data['hdf_data'] = total_data

    # get day cnt and ms cnt for time
    ret = get_day_ms_cnt(hdf_name)
    data['daycnt'] = ret['daycnt']
    data['mscnt'] = ret['mscnt']    
    
    hdf_fd['l1b_obc'].close()
    if (sat=='mwhs'):
        hdf_fd['l1b'].close()

    data['ok'] = True
    return data        

def get_day_ms_cnt(hdf_name):
    hdf_fd = h5.File(hdf_name, 'r')
    data = {'daycnt': None, 'mscnt': None}
    
    # get day cnt and ms cnt for time
    for time_flag, sds_setting in zip(ins_conf.time_sds.keys(), \
                                      ins_conf.time_sds.values()):
        sds = sds_setting['sds']
        if sds not in hdf_fd:
            msg = hdf_name + '`sds ' + sds + ' not exist.'
            common.err(my_log, log_tag, msg)
            hdf_fd.close()
            return data
        
        dataset = hdf_fd[sds]
        if 'real_dims' in sds_setting.keys() \
            and sds_setting['real_dims'] == 2:
            dataset = dataset[:, sds_setting['idx'] ]
            sds_data = common.trans_list_data_for_db(dataset[()], sds_setting)
        else:
            sds_data = common.trans_data_for_db(dataset[()], sds_setting)
            
        data[time_flag] = sds_data
    
    hdf_fd.close()
    return data
    
            
# Insert obc 2 dims data to mysql table.
# Warning: if the talbe exist, just truncate and load again.
# table name like: FY3C_MWTSX_GBAL_L1_20140101_0658_OBCXX_MS, which is the 
# basename of one HDF file.
def obc_2dim_data_to_db(hdf_name):
    # check ins_conf validation
    if( len(ins_conf.obc_to_db) <= 0 ):
        msg = hdf_name + '`There are No obc_to_db setting in conf.'
        common.warn(my_log, log_tag, msg)
        return True
        
    data = get_data_from_hdf(hdf_name, ins_conf.obc_to_db)
    if not data['ok']:
        return False

    scans = data['scans']
    pixels = data['pixels']
    channel = ins_conf.channels

    hdf_data = data['hdf_data']
    time_data = common.get_time_str_from_dayms(data['daycnt'], data['mscnt'],
                                               conf.time_begin[sat])
    
    total_fields = []
    total_fields.extend(ins_conf.obc_to_db.values())
    
    table = common.get_table_name(hdf_name)

    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(table) )
        conn.commit()
        sql = conf.obc_create_table_sql \
            + common.get_create_table_sql(channel, total_fields) + ')'
        common.debug(my_log, log_tag, hdf_name + '`' + sql%table)            
        cur.execute(sql%table)
        conn.commit()
    except MySQLdb.Error, e:
        msg = hdf_name + '`loading 2-dim obc data to db, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False

    # create insert sql tpl, like:
    # 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
    # where id is used for table's primary key.
    showtype = False
    sql_common = common.get_sql_dict(channel, ins_conf.obc_to_db.values(), 
                                     showtype)
    sql = 'insert into ' + table + '(scln, ymdhms, ' + sql_common['field'] \
        + ") values(%s, '%s', " + sql_common['value'] + ')' 
    common.debug(my_log, log_tag, hdf_name + '`' + sql)            

    sds_len = len(ins_conf.obc_to_db)

    for idx in xrange(0, scans):
        # prepare one data to insert, data's sequence MUST follow strictly
        # of sql field sequence.
        one_data = [idx + 1, time_data[idx]]
        for i in xrange(0, sds_len):
            #print i
            #print'----------------'
            #print idx
            #print'******************'
            #print data['hdf_data']
            #print'******************'
            one_data.extend(hdf_data[i][idx][:])
        
        try:
            cur.execute(sql%tuple(one_data))
        except MySQLdb.Error, e:
            msg = hdf_name + '`loading 2-dim obc data to db, Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1] 
            common.err(my_log, log_tag, msg)
            return False

    conn.commit()
    cur.close()
    conn.close()
    return True

# Insert obc 3 dims data to mysql table.
# Warning: if the talbe exist, just truncate and load again.
# table name like: FY3C_MWTSX_GBAL_L1_20140101_0658_OBCXX_MS_13, which is the 
# basename_channel of one HDF file for mwts.
def obc_3dim_data_to_db(hdf_name):
    # check ins_conf validation
    if( len(ins_conf.obc_3dim_to_db) <= 0 ):
        msg = hdf_name + '`There are No obc_3dim_to_db setting in conf.'
        common.warn(my_log, log_tag, msg)
        return True
    
    data = get_data_from_hdf(hdf_name, ins_conf.obc_3dim_to_db)
    if not data['ok']:
        return False

    scans = data['scans']
    pixels = data['pixels']
    channel = ins_conf.channels

    hdf_data = common.avg_data_for_db(data['hdf_data'],ins_conf.obc_3dim_to_db)
    time_data = common.get_time_str_from_dayms(data['daycnt'], data['mscnt'],
                                               conf.time_begin[sat])
    
    total_fields = []
    total_fields.extend(ins_conf.obc_3dim_to_db.values())
    
    table = common.get_table_name(hdf_name) + '_' + str(channel)

    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(table) )
        conn.commit()
        sql = conf.obc_3dim_create_table_sql \
            + common.get_create_table_sql(channel, total_fields) + ', ' \
            + conf.obc_3dim_create_idx_sql + ')'
        common.debug(my_log, log_tag, hdf_name + '`' + sql%table)            
        cur.execute(sql%table)
        conn.commit()
    except MySQLdb.Error, e:
        msg = hdf_name + '`loading 3-dim obc data to db, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False
    
    # create insert sql tpl, like:
    # 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
    # where id is used for table's primary key.
    showtype = False
    dict = common.get_sql_dict(1, ins_conf.obc_3dim_to_db.values(), showtype)
    sql = 'insert into ' + table + '(channel, scln, ymdhms, ' + dict['field'] \
        + ") values(%s, %s, %s, " + dict['value'] + ')'
    common.debug(my_log, log_tag, hdf_name + '`' + sql)            
    
    sds_len = len(ins_conf.obc_3dim_to_db)
    
    
    for idx in xrange(0, scans):
        batch_data = []
        for chnl in xrange(0, channel):
                # prepare data to batch insert every channels count.
                # data's sequence MUST follow strictly of sql field sequence.
            one_data = [chnl + 1, idx + 1, time_data[idx]]
            for i in xrange(0, sds_len):
                
                one_data.extend(hdf_data[i][chnl][idx][:])
            batch_data.append(tuple(one_data))
    

    
        try:
            #print sql
            cur.executemany(sql, batch_data)
        except MySQLdb.Error, e:
            msg = hdf_name + '`loading 3-dim obc data to db, Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1]
            common.err(my_log, log_tag, msg)
            return False

    conn.commit()
    cur.close()
    conn.close()
    return True


def get_scanperiod(path):
    if(sat=='fy3c'and ins=='mwhs'):
        return get_mwhs_scanperiod(path)
    elif(sat=='fy3c'and ins=='mwts'):
        return get_mwts_scanperiod(path)
    else:
        return {'ok':False,'data':'null'}

def get_mwts_scanperiod(path):
    defval={'ok':False,'data':'null'}
    try:      
        hdf = h5.File(path, 'r')  
        source= hdf['Data/ScnlinMillSecond'] 
    except:
        msg = path + '`get_mwts_scanperiod`open hdf failed.'
        common.error(my_log, log_tag, msg)
        return defval
    
    init=0
    list1=source
    list2=[init]*len(list1)
    result=list2
    
    dic={'ok':True,'data':result}
    
    for j in range(0,len(list2)-1):
        if(list1[j+1]<list1[j]):
            list2[j+1]=list1[j+1]+24*60*60*1000-list1[j]
        else:
            list2[j+1]=list1[j+1]-list1[j]
    
    list2[0]=list2[1]
    hdf.close()
    return dic

def get_mwhs_scanperiod(path):
    defval={'ok':False,'data':'null'}
    try:      
        hdf = h5.File(path, 'r')  
        source= hdf['Geolocation/Scnlin_mscnt'] 
    except:
        msg = path + '`get_mwhs_scanperiod`open hdf failed.'
        common.error(my_log, log_tag, msg)        
        return defval
    
    init=0
    list1=source
    list2=[init]*len(list1)
    result=list2

    dic={'ok':True,'data':result}
    
    for j in range(0,len(list2)-1):
        if(list1[j+1]<list1[j]):
            list2[j+1]=list1[j+1]+24*60*60*1000-list1[j]
        else:
            list2[j+1]=list1[j+1]-list1[j]

    list2[0]=list2[1]
    hdf.close()
    return dic

# obc data to db after calc data.
# one table name like: FY3C_MWTSX_GBAL_L1_20140801_1755_OBCXX_MS_CALC
# another table name like: FY3C_MWTSX_GBAL_L1_20140801_1755_OBCXX_MS_CALC_13
def calc_obc_to_db_mwts(hdf_name):
    time_begin = time.time()            
    ret = calc_obc_to_db_mwts_2dim(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 2-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False

    time_begin = time.time()            
    ret = calc_obc_to_db_mwts_3dim(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 3-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False
    
    return True
 
def calc_obc_to_db_mwts_2dim(hdf_name):
    # check calc_obc validation
    if( len(ins_conf.calc_to_db) <= 0 ):
        msg = hdf_name + '`There are No 2-dim calc_obc setting in conf.'
        common.warn(my_log, log_tag, msg)
        return True
        
    attr = common.get_attr_from_hdf(hdf_name, ins_conf)
    if not attr['ok']:
        common.err(my_log, log_tag, attr['msg'])
        return False
    
    scans = attr['scans']
    pixels = attr['pixels']
    channel = ins_conf.channels

    table = common.get_table_name(hdf_name) + '_CALC' # + '_' + str(channel)
    total_fields = []
    total_fields.extend(ins_conf.calc_to_db.values())
    
    # create table.
    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(table) )
        conn.commit()
        sql = conf.obc_create_table_sql \
            + common.get_create_table_sql(channel, total_fields) + ')'
        common.debug(my_log, log_tag, hdf_name + '`' + sql%table)            
        cur.execute(sql%table)
        conn.commit()
    except MySQLdb.Error, e:
        msg = hdf_name + '`loading 2-dim obc calc data, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False
    
    # create insert sql tpl, like:
    # 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
    # where id is used for table's primary key.
    sql_common = common.get_sql_dict(channel, ins_conf.calc_to_db.values(), 
                                     showtype = False)
    sql = 'insert into ' + table + '(scln, ymdhms, ' + sql_common['field'] \
        + ") values(%s, '%s', " + sql_common['value'] + ')' 
    common.debug(my_log, log_tag, hdf_name + '`' + sql)            

    # get day cnt and ms cnt for time
    day_ms = get_day_ms_cnt(hdf_name)
    time_data = common.get_time_str_from_dayms(day_ms['daycnt'], day_ms['mscnt'],
                                               conf.time_begin[sat])

    # get obc calc data from hdf. this is different with each instrument.
    try:
        hdf_fd = h5.File(hdf_name, 'r')
        cold_ang = hdf_fd['Data/Cold_Sky_Angle']
        hot_ang = hdf_fd['Data/Hot_Load_Angle']
        earth_ang = hdf_fd['Data/Earth_Obs_Angle']
        
        cold_minus = map(common.near_int, 
                         [x*1000 for x in (cold_ang[:, 1] - cold_ang[:, 0]) ] )
        hot_minus = map(common.near_int, 
                         [x*1000 for x in (hot_ang[:, 1] - hot_ang[:, 0]) ] )
        earth_minus = map(common.near_int, 
                         [x*1000 for x in (earth_ang[:, 1] - earth_ang[:, 0])])
    except:
        msg = hdf_name + '`calc 2-dim obc data failed'
        common.err(my_log, log_tag, msg)
        return False
    hdf_fd.close()

    scan_data = get_scanperiod(hdf_name)
    if scan_data['ok'] is not True:
        msg = hdf_name + '`get_scanperiod failed when calc 2-dim obc data'
        common.err(my_log, log_tag, msg)
        return False
    else:
        scan_period = scan_data['data']
    
    # insert to db.    
    for idx in xrange(0, scans):
        # prepare one data to insert
        one_data = [idx + 1, time_data[idx], cold_minus[idx],
                    hot_minus[idx], earth_minus[idx], scan_period[idx]]
        try:
            cur.execute(sql%tuple(one_data))
        except MySQLdb.Error, e:
            msg = hdf_name + '`loading 2-dim calc obc data, Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1] 
            common.err(my_log, log_tag, msg)
            return False

    conn.commit()
    cur.close()
    conn.close()
    return True

def calc_obc_to_db_mwts_3dim(hdf_name):
    return True

def calc_obc_to_db_mwhs(hdf_name):
    time_begin = time.time()            
    ret = calc_obc_to_db_mwhs_2dim(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 2-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False

    time_begin = time.time()            
    ret = calc_obc_to_db_mwhs_3dim(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 3-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False
    
    return True
 
def calc_obc_to_db_mwhs_2dim(hdf_name):
    # check calc_obc validation
    if( len(ins_conf.calc_to_db) <= 0 ):
        msg = hdf_name + '`There are No 2-dim calc_obc setting in conf.'
        common.warn(my_log, log_tag, msg)
        return True
        
    attr = common.get_attr_from_hdf(hdf_name, ins_conf)
    if not attr['ok']:
        common.err(my_log, log_tag, attr['msg'])
        return False
    
    scans = attr['scans']
    pixels = attr['pixels']
    channel = ins_conf.channels

    table = common.get_table_name(hdf_name) + '_CALC' # + '_' + str(channel)
    total_fields = []
    total_fields.extend(ins_conf.calc_to_db.values())
    
    # create table.
    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(table) )
        conn.commit()
        sql = conf.obc_create_table_sql \
            + common.get_create_table_sql(channel, total_fields) + ')'
        common.debug(my_log, log_tag, hdf_name + '`' + sql%table)            
        cur.execute(sql%table)
        conn.commit()
    except MySQLdb.Error, e:
        msg = hdf_name + '`loading 2-dim obc calc data, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False
    
    # create insert sql tpl, like:
    # 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
    # where id is used for table's primary key.
    sql_common = common.get_sql_dict(channel, ins_conf.calc_to_db.values(), 
                                     showtype = False)
    sql = 'insert into ' + table + '(scln, ymdhms, ' + sql_common['field'] \
        + ") values(%s, '%s', " + sql_common['value'] + ')' 
    common.debug(my_log, log_tag, hdf_name + '`' + sql)            

    # get day cnt and ms cnt for time
    day_ms = get_day_ms_cnt(hdf_name)
    time_data = common.get_time_str_from_dayms(day_ms['daycnt'], day_ms['mscnt'],
                                               conf.time_begin[sat])

    # get obc calc data from hdf. this is different with each instrument.
    try:
        hdf_fd = h5.File(hdf_name, 'r')
        digital_control_u=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['Digital_Contrlo_Unit']
        cell_control_u=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['Cell_Control_Unit']
        motor_temp_1=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['Motor_Temperature_1']
        motor_temp_2=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['Motor_Temperature_2']
        antenna_mask_temp_1=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['Antenna_Mask_Temperature_1']
        antenna_mask_temp_2=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['Antenna_Mask_Temperature_2']
        fet_118_amp_temp=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['FET_118GHz_Amplifier_Temperature']
        fet_183_amp_temp=hdf_fd['GROUP_TABLE/Instrument Performance Vdata']['FET_183GHz_Amplifier_Temperature']
    except:
        msg = hdf_name + '`calc 2-dim obc data failed'
        common.err(my_log, log_tag, msg)
        return False
    hdf_fd.close()

    scan_data = get_scanperiod(hdf_name)
    if scan_data['ok'] is not True:
        msg = hdf_name + '`get_scanperiod failed when calc 2-dim obc data'
        common.err(my_log, log_tag, msg)
        return False
    else:
        scan_period = scan_data['data']
    
   
    
    
    # insert to db.    
    for idx in xrange(0, scans):
        # prepare one data to insert
        one_data = [idx + 1, time_data[idx], digital_control_u[idx]*1000,
                    cell_control_u[idx]*1000, motor_temp_1[idx]*1000, motor_temp_2[idx]*1000, antenna_mask_temp_1[idx]*1000, antenna_mask_temp_2[idx]*1000, 
                    fet_118_amp_temp[idx]*1000, fet_183_amp_temp[idx]*1000, scan_period[idx]]
        try:
            cur.execute(sql%tuple(one_data))
        except MySQLdb.Error, e:
            msg = hdf_name + '`loading 2-dim calc obc data, Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1] 
            common.err(my_log, log_tag, msg)
            return False

    conn.commit()
    cur.close()
    conn.close()
    return True

def calc_obc_to_db_mwhs_3dim(hdf_name):
    # check ins_conf validation
    if( len(ins_conf.calc_3dim_to_db) <= 0 ):
        msg = hdf_name + '`There are No calc_3dim_to_db setting in conf.'
        common.warn(my_log, log_tag, msg)
        return True
    
    attr = common.get_attr_from_hdf(hdf_name, ins_conf)
    if not attr['ok']:
        common.err(my_log, log_tag, attr['msg'])
        return False

    scans = attr['scans']
    pixels = attr['pixels']
    channel = ins_conf.channels
    
    table = common.get_table_name(hdf_name) + '_CALC' + '_' + str(channel)
    total_fields = []
    total_fields.extend(ins_conf.calc_3dim_to_db.values())
    
    

    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(table) )
        conn.commit()
        sql = conf.obc_3dim_create_table_sql \
            + common.get_create_table_sql(channel, total_fields) + ', ' \
            + conf.obc_3dim_create_idx_sql + ')'
        common.debug(my_log, log_tag, hdf_name + '`' + sql%table)            
        cur.execute(sql%table)
        conn.commit()
    except MySQLdb.Error, e:
        msg = hdf_name + '`loading 3-dim obc data to db, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False
    
    # create insert sql tpl, like:
    # 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
    # where id is used for table's primary key.
    
    sql_common = common.get_sql_dict(1, ins_conf.calc_3dim_to_db.values(), 
                                     showtype = False)
    sql = 'insert into ' + table + '(channel, scln, ymdhms, ' + sql_common['field'] \
        + ") values(%s, %s, %s, " + sql_common['value'] + ')' 
    common.debug(my_log, log_tag, hdf_name + '`' + sql)            

    # get day cnt and ms cnt for time
    day_ms = get_day_ms_cnt(hdf_name)
    time_data = common.get_time_str_from_dayms(day_ms['daycnt'], day_ms['mscnt'],
                                               conf.time_begin[sat])
    try:
        
        gain=get_mwhs_gain(hdf_name, channel)
        
    except:
        msg = hdf_name + '`calc 2-dim obc data failed'
        common.err(my_log, log_tag, msg)
        return False
    
              
     # get obc calc data from hdf. this is different with each instrument.
    try:
        hdf_fd = h5.File(hdf_name, 'r')
        AGC = hdf_fd['Calibration/AGC']
        SPBB= hdf_fd['Calibration/SPBB_DN_Avg']
    except:
        msg = hdf_name + '`calc 3-dim calc obc data failed'
        common.err(my_log, log_tag, msg)
        return False
    
    
    AGC3=[[0 for j in range(scans)] for i in range(channel)]
    SPBB1=[[0 for j in range(scans)] for i in range(channel)]
    SPBB2=[[0 for j in range(scans)] for i in range(channel)]
    
    for i in range(0,channel):
        for j in range(0,scans):
            AGC3[i][j]=AGC[j][i]
            SPBB1[i][j]=SPBB[j][i]
            SPBB2[i][j]=SPBB[j][i+15]

    for idx in xrange(0, scans):
        batch_data = []
        for chnl in xrange(0, channel):
            # prepare data to batch insert every channels count.
            # data's sequence MUST follow strictly of sql field sequence.
            one_data = [chnl + 1, idx + 1, time_data[idx],gain[chnl][idx]*1000,AGC3[chnl][idx],SPBB1[chnl][idx]*1000,SPBB2[chnl][idx]*1000]
            
            batch_data.append(tuple(one_data))
        try:
            cur.executemany(sql, batch_data)
        except MySQLdb.Error, e:
            msg = hdf_name + '`loading 3-dim calc obc data to db, Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1]
            common.err(my_log, log_tag, msg)
            return False
        
    hdf_fd.close()
    conn.commit()
    cur.close()
    conn.close()
    return True

def calc_obc_to_db_mwri(hdf_name):
    time_begin = time.time()            
    ret = calc_obc_to_db_mwri_2dim(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 2-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False

    time_begin = time.time()            
    ret = calc_obc_to_db_mwri_3dim(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 3-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False
    
    return True
 
def calc_obc_to_db_mwri_2dim(hdf_name):
    return True

def calc_obc_to_db_mwri_3dim(hdf_name):
    # check ins_conf validation
    if( len(ins_conf.calc_3dim_to_db) <= 0 ):
        msg = hdf_name + '`There are No calc_3dim_to_db setting in conf.'
        common.warn(my_log, log_tag, msg)
        return True
    
    attr = common.get_attr_from_hdf(hdf_name, ins_conf)
    if not attr['ok']:
        common.err(my_log, log_tag, attr['msg'])
        return False

    scans = attr['scans']
    pixels = attr['pixels']
    channel = ins_conf.channels
    
    table = common.get_table_name(hdf_name) + '_CALC' + '_' + str(channel)
    total_fields = []
    total_fields.extend(ins_conf.calc_3dim_to_db.values())

    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(table) )
        conn.commit()
        #print total_fields
        sql = conf.obc_3dim_create_table_sql \
            + common.get_create_table_sql(channel, total_fields) + ', ' \
            + conf.obc_3dim_create_idx_sql + ')'
        print sql
        
    
        common.debug(my_log, log_tag, hdf_name + '`' + sql%table)            
        cur.execute(sql%table)
        conn.commit()
    except MySQLdb.Error, e:
        msg = hdf_name + '`loading 3-dim obc data to db, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False
    
    # create insert sql tpl, like:
    # 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
    # where id is used for table's primary key.
    
    sql_common = common.get_sql_dict(1, ins_conf.calc_3dim_to_db.values(), 
                                     showtype = False)
    sql = 'insert into ' + table + '(channel, scln, ymdhms, ' + sql_common['field'] \
        + ") values(%s, %s, %s, " + sql_common['value'] + ')' 
    common.debug(my_log, log_tag, hdf_name + '`' + sql)            

    # get day cnt and ms cnt for time
    day_ms = get_day_ms_cnt(hdf_name)
    time_data = common.get_time_str_from_dayms(day_ms['daycnt'], day_ms['mscnt'],
                                               conf.time_begin[sat])
    try:
        
        SBT=get_mwri_synthesize_temp(hdf_name, channel)
        
    except:
        msg = hdf_name + '`calc synthesize_temp data failed'
        common.err(my_log, log_tag, msg)
        return False
    
              
     # get obc calc data from hdf. this is different with each instrument.
    try:
        hdf_fd = h5.File(hdf_name, 'r')
        AGC = hdf_fd['Calibration/AGC_Control_Volt_Count_10-89GHz']
        ABCC= hdf_fd['Calibration/ANTENNA_BT_CALIBRATION_COEF(SCALE+OFFSET)']
        
    except:
        msg = hdf_name + '`calc 3-dim calc obc data failed'
        common.err(my_log, log_tag, msg)
        return False
    
    
    AGC3=[[0 for j in range(scans)] for i in range(channel)]
    ABCC1=[[0 for j in range(scans)] for i in range(channel)]
    ABCC2=[[0 for j in range(scans)] for i in range(channel)]
    
    for i in range(0,channel):
        for j in range(0,scans):
            AGC3[i][j]=AGC[j][i]
            ABCC1[i][j]=ABCC[j][2*i]
            ABCC2[i][j]=ABCC[j][2*i+1]

    for idx in xrange(0, scans):
        batch_data = []
        for chnl in xrange(0, channel):
            # prepare data to batch insert every channels count.
            # data's sequence MUST follow strictly of sql field sequence.
            one_data = [chnl + 1, idx + 1, time_data[idx],AGC3[chnl][idx],ABCC1[chnl][idx]*1000000,ABCC2[chnl][idx]*1000000,SBT[chnl][idx]]
            
            batch_data.append(tuple(one_data))
        try:
            cur.executemany(sql, batch_data)
        except MySQLdb.Error, e:
            msg = hdf_name + '`loading 3-dim calc obc data to db, Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1]
            common.err(my_log, log_tag, msg)
            return False
        
    hdf_fd.close()
    conn.commit()
    cur.close()
    conn.close()
    return True


def calc_obc_to_db(hdf_name):
    if(sat=='fy3c'and ins=='mwts'):
        return calc_obc_to_db_mwts(hdf_name)
    if(sat=='fy3c'and ins=='mwhs'):
        return calc_obc_to_db_mwhs(hdf_name)
    if(sat=='fy3c'and ins=='mwri'):
        return calc_obc_to_db_mwri(hdf_name)
    if(sat=='fy3c'and ins=='iras'):
        return calc_obc_to_db_iras(hdf_name)
    
    return True
    
def calc_nedn_to_db(hdf_name):
    
    
    pool = Pool()
    input = zip([hdf_name]*ins_conf.channels, xrange(1, ins_conf.channels + 1), 
                [sat]*ins_conf.channels, [ins]*ins_conf.channels)
  
    pool_ret = pool.map(do_nedn_one_channel, input)
    pool.close()
    pool.join()



#     for channel in range(1,ins_conf.channels+1):
#         input = (hdf_name,channel,sat, ins)
#         do_nedn_one_channel(input)
        

     
    if False in pool_ret:
        return False
    else:
        return True
    
def do_nedn_one_channel(input):
    
    
    (hdf_name, channel, mysat, myins) = input
    nedn = 65535.0
    if(mysat=='fy3c'and myins=='mwts'):
        nedn = get_mwts(hdf_name, channel)
    if(mysat=='fy3c'and myins=='mwhs'):
        nedn = get_mwhs(hdf_name, channel)
    if(mysat=='fy3c'and myins=='mwri'):
        nedn = get_mwri(hdf_name, channel)
    if(mysat=='fy3c'and myins=='iras'):
        return True
#         nedn = get_iras(hdf_name, channel)
                
    nedn = float(nedn)

    # insert nedn to db [ ymdh, channel, nedn ]
    filename = os.path.basename(hdf_name) # FY3C_MWTSX_GBAL_L1_20131003_1641
    ymd = filename[ins_conf.pre_year : ins_conf.pre_year + len('20141003')]
    hm = filename[ins_conf.pre_hour : ins_conf.pre_hour + len('2135')]
    ymdh = ymd[0:4] + '-' + ymd[4:6] + '-' + ymd[6:8] + ' ' + hm[0:2] \
           + ':' + hm[2:4] + ':00'
    insert_sql = 'replace into ' + conf.table_setting[sat][ins]['nedn_table'] \
                + " values('" + ymdh + "', " + str(channel) + ', ' \
                + format(nedn, '.3f') + ')'
    
    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.db_setting['stat_db'])
        cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()
    except MySQLdb.Error as e:
        msg = hdf_name + '`' + str(channel) + '`do_nedn_one_channel, ' \
            + 'Mysql Fatal Error[' + str(e.args[0])+']: ' + e.args[1] 
        common.err(my_log, log_tag, msg)
        return False
    else:
        return True

def get_mwri(path,channel):
    defval=65535.0
    try:      
        hdf = h5.File(path, 'r')  
        ColdCount= hdf['Calibration/SP_IT_CAL_OBS_COUNT'] 
        Calibration= hdf['Calibration/ANTENNA_BT_CALIBRATION_COEF(SCALE+OFFSET)']
        NOscan=hdf.attrs['Number Of Scans']
    except:
        msg = path + '`' + str(channel) + '`open hdf failed' \
            + ', use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval
    
    if(NOscan<200):
        msg = path + '`' + str(channel) + '`scan number[' + str(NOscan) \
            + '] less then 200, use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval

    init=0   
    Gain=[init]*200
    Ci=[init]*200
    Gi=[init]*200
    
    Cstd=[init]*10
    Gavg=[init]*10
    
    deltT=[init]*10
    temp=0
    Gtotal=[init]*10
    for i in range(0,200):
        Ci[i]=(float(ColdCount[15][i][channel-1])+float(ColdCount[16][i][channel-1]))/2
        Gi[i]=float(Calibration[i][2*channel-2])
        
    for i in range(0,10):
        Cstd[i]=numpy.std(Ci[20*i:20*(i+1)], dtype=numpy.float64)
        Gavg[i]=numpy.mean(Gi[20*i:20*(i+1)], dtype=numpy.float64)
    for i in range(0,10):
        deltT[i]=Gavg[i]*Cstd[i]
    
    deltT.sort()
    hdf.close()
    if(deltT[8]<0):
        msg = hdf_name + '`' + str(channel) + '`calc nedn is ' + str(deltT[8])\
            + ', less then 0, use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval
    else:
        return float(deltT[8])
            
def get_iras(path,channel):
    defcold=[65535]*24
    defhot=[65535]*24
    defval={'ok':False,'Cold':defcold,'Hot':defhot}
    try:      
        hdf = h5.File(path, 'r')  
        index= hdf['Telemetry/ira_mirdir_sign'] 
        HCC= hdf['Data/IRAS_DN'] 
        Calibration= hdf['Data/ira_calcoef']
    except:
        msg = hdf_name + '`' + str(channel) + '`open hdf failed' \
            + ', use nedn default value: 65535'
        common.error(my_log, log_tag, msg)
        return defval
    
    Hindex=[]
    Cindex=[]
    HT=[]
    CT=[]
    
    try:
        for i in range(0,len(index)):
            if(index[i]==204):
                Hindex.append(i)
            elif(index[i]==187):
                Cindex.append(i)
        
        for i in range(0,len(Hindex)):
            HT.append(numpy.std(HCC[channel-1][Hindex[i]][0:45], dtype=numpy.float64)*Calibration[Hindex[i]][channel-1][1])
        
        for i in range(0,len(Hindex)):
            CT.append(numpy.std(HCC[channel-1][Cindex[i]][0:45], dtype=numpy.float64)*Calibration[Cindex[i]][channel-1][1])
        
        dic={'ok':True,'Cold':CT,'Hot':HT}
    except:
        msg = hdf_name + '`' + str(channel) + '`file is not complete' \
            + ', Column data can not be accessed'
        common.error(my_log, log_tag, msg)
        hdf.close()
        return defval
    
    return dic
        
def get_mwts(hdf_name, channel):
    defval=65535.0
    try:      
        hdf = h5.File(hdf_name, 'r')  
        Csc= hdf['Data/Cold_Sky_Count'] 
        Hlc= hdf['Data/Hot_Load_Count']
        Tbw= hdf['Data/Hot_Load_Temp_Avg']
        NOscan=hdf.attrs['Number Of Scans'][0]
    except:
        msg = hdf_name + '`' + str(channel) + '`open hdf failed' \
            + ', use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval

    if(NOscan<210):
        msg = hdf_name + '`' + str(channel) + '`scan number[' + str(NOscan) \
            + '] less then 210, use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval

    Tbc=2.73 
    init=0   
    G=[init]*200
    DNwi=[init]*200
    DNci=[init]*200
    DNwid=[init]*10
    DNcid=[init]*10
    deltT=[init]*10
    temp=0
    Gtotal=[init]*10
    for i in range(1,201):
        DNw=(int(Hlc[channel-1][i+8][6])+int(Hlc[channel-1][i+8][7]))/2
        DNc=(int(Csc[channel-1][i+8][6])+int(Csc[channel-1][i+8][7]))/2
        DNwi[i-1]=DNw
        DNci[i-1]=DNc
        G[i-1]=(DNw-DNc)/(Tbw[i+8]-Tbc)
    for j in range(1,201):    
        temp=temp+G[j-1]/20
        if(j%20==0):    
            Gtotal[j/20-1]=temp
            temp=0
    for    i in range(0,10):
        DNwid[i]=numpy.std(DNwi[20*i:20*(i+1)], dtype=numpy.float64)
        DNcid[i]=numpy.std(DNci[20*i:20*(i+1)], dtype=numpy.float64)
    for i in range(0,10):
        if(Gtotal[i]==0):
            deltT[i]=defval
        else:
            deltT[i]=pow((DNwid[i]*DNwid[i]+DNcid[i]*DNcid[i])/2,0.5)/Gtotal[i]
    
    deltT.sort()
    if(deltT[7]<0):
        msg = hdf_name + '`' + str(channel) + '`calc nedn is ' + str(deltT[7])\
             + ', less then 0, use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval
    else:
        return float(deltT[7])
def get_mwhs_gain(hdf_name, channel):
    
    defval=65535.0
    
    try:      
        hdf = h5.File(hdf_name, 'r')  
        C= hdf['Calibration/SPBB_DN_Avg'] 
        Th= hdf['Calibration/PRT_Tavg']
        NOscan=hdf.attrs['Count_scnlines'][0]
        dev=[[defval for j in range(NOscan)] for i in range(channel)]
    except:
        msg = hdf_name + '`' + str(channel) + '`open hdf failed' \
            + ', use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return dev
    
    if(NOscan<400):
        msg = hdf_name + '`' + str(channel) + '`scan number[' + str(NOscan) \
            + '] less then 400, use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return dev
    
    Tl=2.73 
    init=0   
    G=[[init for j in range(NOscan)] for i in range(channel)]
    Thi=[init for j in range(NOscan)]
    Chi=[init for j in range(NOscan)]
    Cli=[init for j in range(NOscan)]
    
    for c in range(0,channel):
        for i in range(1,NOscan+1):
            if(channel<10):
                Thi[i-1]=Th[i-1][0]
            else:
                Thi[i-1]=Th[i-1][1]
            Chi[i-1]=C[i-1][c]
            Cli[i-1]=C[i-1][c+15]

            if(Chi[i-1]-Cli[i-1]==0):
                G[c][i-1]=defval
            else:
                G[c][i-1]=(Chi[i-1]-Cli[i-1])/(Thi[i-1]-Tl)
                
                        
        
    return G

def get_mwhs(hdf_name, channel):
    defval=65535.0
    try:      
        hdf = h5.File(hdf_name, 'r')  
        C= hdf['Calibration/SPBB_DN_Avg'] 
        Th= hdf['Calibration/PRT_Tavg']
        NOscan=hdf.attrs['Count_scnlines'][0]
    except:
        msg = hdf_name + '`' + str(channel) + '`open hdf failed' \
            + ', use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval
    
    if(NOscan<400):
        msg = hdf_name + '`' + str(channel) + '`scan number[' + str(NOscan) \
            + '] less then 400, use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval
    
    Tl=2.73 
    init=0   
    G=[init]*200
    Thi=[init]*400
    Chi=[init]*400
    Cli=[init]*400
    Chrms=[init]*10
    Clrms=[init]*10
    Thavg=[init]*10
    Chavg=[init]*10
    Clavg=[init]*10
    deltT=[init]*10
    
    for i in range(1,401):
        if(channel<10):
            Thi[i-1]=Th[i-1][0]
        else:
            Thi[i-1]=Th[i-1][1]
        Chi[i-1]=C[i-1][channel-1]
        Cli[i-1]=C[i-1][channel+14]
        
    for i in range(0,10):
        Chrms[i]=numpy.std(Chi[40*i:40*(i+1)], dtype=numpy.float64)
        Clrms[i]=numpy.std(Cli[40*i:40*(i+1)], dtype=numpy.float64)
        Thavg[i]=numpy.mean(Thi[40*i:40*(i+1)], dtype=numpy.float64)
        Chavg[i]=numpy.mean(Chi[40*i:40*(i+1)], dtype=numpy.float64)
        Clavg[i]=numpy.mean(Cli[40*i:40*(i+1)], dtype=numpy.float64)
        
    for i in range(0,10):
        if(Chavg[i]-Clavg[i]==0):
            deltT[i]=defval
        else:
            deltT[i]=(Thavg[i]-Tl)/(Chavg[i]-Clavg[i])*pow((Chrms[i]*Chrms[i]+Clrms[i]*Clrms[i])/2,0.5)        
        
    deltT.sort()
    if(deltT[8]<0):
        msg = hdf_name + '`' + str(channel) + '`calc nedn is ' + str(deltT[8])\
             + ', less then 0, use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval
    else:
        return float(deltT[8])


                
def get_mwri_synthesize_temp(path,channel):                
        defval=65535
        try:
                hdf = h5.File(path, 'r')     
                obsc=hdf['Calibration/SP_IT_CAL_OBS_COUNT']
                calib=hdf['Calibration/ANTENNA_BT_CALIBRATION_COEF(SCALE+OFFSET)']
                hotc=hdf['Calibration/Hotloadreflector_CSM_Hotload_Temp_Count']
                NOscan=hdf.attrs['Number Of Scans'][0]
                dev=[[defval for j in range(NOscan)] for i in range(channel)]
                #hdf.close()
        except:
                print 'erro! file is not available!'
                return dev
        init=0 
        
        G=[[init for j in range(NOscan)] for i in range(channel)]    
        
                
        for c in range(0,channel):
            for i in range(0,NOscan):
                if(c==1 or c==4):
                    cl=9
                elif(c==2):
                    cl=10
                elif(c==3 or c==6):
                    cl=8
                elif(c==5 or c==8 or c==10):
                    cl=6
                elif(c==7 or c==9):
                    cl=5
                else:
                    cl=10
                G[c][i]=obsc[cl][i][c-1]*calib[i][2*c-2]+calib[i][2*c-1]-hotc[i][4]
                
        #result=[]
        #for i in range(0,len(obsc[cl])):
        #        result.append([])
        #        result[i].append(obsc[cl][i][channel-1]*calib[i][2*channel-2]+calib[i][2*channel-1]-hotc[i][4])
        return G        
                



# Insert obc data to mysql table.
# Warning: if the talbe exist, just truncate and load again.
# table name like: FY3C_MWTSX_GBAL_L1_20140101_0658_OBCXX_MS, which is the 
# basename of one HDF file.
def obc_data_to_db(hdf_name):
    time_begin = time.time()            
    ret = obc_2dim_data_to_db(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 2-dim data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False

    time_begin = time.time()            
    ret = obc_3dim_data_to_db(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 3-dim data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)
    if not ret:
        return False

    
    time_begin = time.time()            
    ret = calc_nedn_to_db(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`calc nedt finished.`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False

    time_begin = time.time()         
    ret = calc_obc_to_db(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`calc obc finished.`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        return False
    
    return True

# main loop
def main():
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, 'program start. pid=' + str(pid))
    
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)  
    
    while True:
        # tell daemon that i am alive at current time.
        cur_time = common.utc_nowtime()
        common.wt_file(my_alivefile, cur_time)
        
        hdf = common.get_files(conf.input_hdf_path, ins_conf.OBC, '.OK', 'hdf')
        
        for one_hdf in hdf:
            cur_time = common.utc_nowtime()
            common.wt_file(my_alivefile, cur_time)
            
            # check *OBC.OK file, avoid redo.
            obc_ok_file = one_hdf + '.OBC.OK'
            if os.path.isfile(obc_ok_file) or not os.path.isfile(one_hdf+'.OK'):
                continue

            msg = one_hdf + '`loading to db now'
            common.info(my_log, log_tag, msg)
        
            time_begin = time.time()
            ret = obc_data_to_db(one_hdf)
            time_end = time.time()
            timeuse = str(round(time_end - time_begin, 2))

            if ret:
                common.wt_file(obc_ok_file, obc_ok_file)
                msg = one_hdf + '`SUCC`load obc data to db finished.' \
                    + '`timeuse=' + timeuse
                common.info(my_log, log_tag, msg)
                # arch hdf
                info = common.get_filename_year_mon(one_hdf, ins_conf)       
                arch = conf.arch_hdf_path + '/' + sat.upper() + '/' \
                     + ins.upper() + '/' + info[1] + '/' + info[2]
                common.mv_hdf(obc_ok_file, arch)
                common.mv_hdf(one_hdf + '.OK', arch)
                common.mv_hdf(one_hdf, arch)
            else:
                msg = one_hdf + '`FAILED`load error, please redo manually.' \
                    + '`timeuse=' + timeuse
                common.err(my_log, log_tag, msg)
                common.mv_hdf(one_hdf + '.OK', conf.redo_hdf_path)
                common.mv_hdf(one_hdf, conf.redo_hdf_path)

        my_sleep_time = common.get_sleep_time()
        for i in range(0, my_sleep_time, 1):
            time.sleep(1)

if __name__ == '__main__':
    main()


