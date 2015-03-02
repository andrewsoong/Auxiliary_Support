#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""All day run program, put OBC hdf data to db.

Usage:
    obc_to_db.py --sat=mysat --ins=myins

Arguments:
    sat: the satellite you want to calc
    ins: the insatrument you want to calc

"""

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
            print msg
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
            + common.get_create_table_sql(1, total_fields) + ', ' \
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
    elif(sat=='fy3c'and ins=='iras'):
        return get_iras_scanperiod(path)
    else:
        return {'ok':False,'data':'null'}

def get_iras_scanperiod(path):
    defval={'ok':False,'data':'null'}
    try:      
        hdf = h5.File(path, 'r')  
        source= hdf['Data/Scnlin_mscnt'] 
    except:
        msg = path + '`get_iras_scanperiod`open hdf failed.'
        common.error(my_log, log_tag, msg)
        print msg
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

def calc_obc_to_db_iras_2dim(hdf_name):
    # check calc_obc validation
    if( len(ins_conf.calc_to_db) <= 0 ):
        msg = hdf_name + '`There are No 2-dim calc_obc setting in conf.'
        common.warn(my_log, log_tag, msg)
        print msg
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
        print sql%table           
        cur.execute(sql%table)
        conn.commit()
        print 'commit sucess!!!'
    except MySQLdb.Error, e:
        print 'sql error !!!!'
        msg = hdf_name + '`loading 2-dim obc calc data, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        print msg
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
    try:
        hdf = h5.File(hdf_name, 'r')
        ira_black_temp = iras_calc_for_ira_black_temp(hdf['Telemetry/ira_black_temp'])
        ira_modulator_temp = iras_calc_for_ira_modulator_temp(hdf['Telemetry/ira_modulator_temp'])
        ira_wheel_temp = iras_calc_for_ira_wheel_temp(hdf['Telemetry/ira_wheel_temp'])
        ira_parts_temp = iras_calc_for_ira_parts_temp(hdf['Telemetry/ira_parts_temp'])
        ira_colder_temp = iras_calc_for_ira_colder_temp(hdf['Telemetry/ira_colder_temp'])
        ira_second_power = iras_calc_for_ira_second_power(hdf['Telemetry/ira_second_power'])
        ira_pc_power = iras_calc_for_ira_pc_power(hdf['Telemetry/ira_pc_power'])
        ira_turn = iras_calc_for_ira_turn(hdf['Telemetry/ira_turn'])
        ira_temp_control = iras_calc_for_ira_temp_control(hdf['Telemetry/ira_temp_control'])
        
    except:
        print 'read hdf error !!!'
        msg = hdf_name + '`calc 2-dim obc data failed'
        common.err(my_log, log_tag, msg)
        print 'calc 2-dim obc data failed !!!!!!'
        return False
    hdf.close()
    
    scan_data = get_scanperiod(hdf_name)
    if scan_data['ok'] is not True:
        msg = hdf_name + '`get_scanperiod failed when calc 2-dim obc data'
        common.err(my_log, log_tag, msg)
        print msg
        return False
    else:
        scan_period = scan_data['data']
    
    # insert to db.    
    for idx in xrange(0, scans):
        # prepare one data to insert
        one_data = [idx + 1, time_data[idx], str(ira_black_temp[idx][0]*1000), str(ira_black_temp[idx][1]*1000),str(ira_black_temp[idx][2]*1000),str(ira_black_temp[idx][3]*1000),
                    str(ira_modulator_temp[idx][0]*1000),str(ira_modulator_temp[idx][1]*1000),str(ira_modulator_temp[idx][2]*1000),str(ira_modulator_temp[idx][3]*1000),
                    str(ira_wheel_temp[idx][0]*1000),str(ira_wheel_temp[idx][1]*1000),str(ira_wheel_temp[idx][2]*1000),str(ira_wheel_temp[idx][3]*1000),
                    str(ira_parts_temp[idx][0]*1000),str(ira_parts_temp[idx][1]*1000),str(ira_parts_temp[idx][2]*1000),str(ira_parts_temp[idx][3]*1000),str(ira_parts_temp[idx][4]*1000),
                    str(ira_colder_temp[idx][0]*1000),str(ira_colder_temp[idx][1]*1000),
                    str(ira_second_power[idx][0]*1000),str(ira_second_power[idx][1]*1000),str(ira_second_power[idx][2]*1000),
                    str(ira_pc_power[idx][0]*1000),str(ira_pc_power[idx][1]*1000),str(ira_pc_power[idx][2]*1000),str(ira_pc_power[idx][3]*1000),
                    str(ira_turn[idx][0]*1000),str(ira_turn[idx][1]*1000),str(ira_turn[idx][2]*1000),
                    str(ira_temp_control[idx][0]*1000)]
        try:
            cur.execute(sql%tuple(one_data))
        except MySQLdb.Error, e:
            print 'inser error !!!!!!'
            msg = hdf_name + '`loading 2-dim calc obc data, Mysql Fatal Error['\
                + str(e.args[0])+']: '+e.args[1] 
            common.err(my_log, log_tag, msg)
            print msg
            return False

    conn.commit()
    cur.close()
    conn.close()
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

#     # get obc calc data from hdf. this is different with each instrument.
#     try:
#         hdf_fd = h5.File(hdf_name, 'r')
#         cold_ang = hdf_fd['Data/Cold_Sky_Angle']
#         hot_ang = hdf_fd['Data/Hot_Load_Angle']
#         earth_ang = hdf_fd['Data/Earth_Obs_Angle']
#         
#         cold_minus = map(common.near_int, 
#                          [x*1000 for x in (cold_ang[:, 1] - cold_ang[:, 0]) ] )
#         hot_minus = map(common.near_int, 
#                          [x*1000 for x in (hot_ang[:, 1] - hot_ang[:, 0]) ] )
#         earth_minus = map(common.near_int, 
#                          [x*1000 for x in (earth_ang[:, 1] - earth_ang[:, 0])])
#     except:
#         msg = hdf_name + '`calc 2-dim obc data failed'
#         common.err(my_log, log_tag, msg)
#         return False
#     hdf_fd.close()

    try:
        hdf = h5.File(hdf_name, 'r')
        ira_black_temp = iras_calc_for_ira_black_temp(hdf['Telemetry/ira_black_temp'])
        ira_modulator_temp = iras_calc_for_ira_modulator_temp(hdf['Telemetry/ira_modulator_temp'])
        ira_wheel_temp = iras_calc_for_ira_wheel_temp(hdf['Telemetry/ira_wheel_temp'])
        ira_parts_temp = iras_calc_for_ira_parts_temp(hdf['Telemetry/ira_parts_temp'])
        ira_colder_temp = iras_calc_for_ira_colder_temp(hdf['Telemetry/ira_colder_temp'])
        ira_second_power = iras_calc_for_ira_second_power(hdf['Telemetry/ira_second_power'])
        ira_pc_power = iras_calc_for_ira_pc_power(hdf['Telemetry/ira_pc_power'])
        ira_turn = iras_calc_for_ira_turn(hdf['Telemetry/ira_turn'])
        ira_temp_control = iras_calc_for_ira_temp_control(hdf['Telemetry/ira_temp_control'])
    except:
        msg = hdf_name + '`calc 2-dim obc data failed'
        common.err(my_log, log_tag, msg)
        return False
    hdf.close()



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
        one_data = [idx + 1, time_data[idx], ira_black_temp[idx],
                    ira_modulator_temp[idx], ira_wheel_temp[idx], ira_parts_temp[idx],
                    ira_colder_temp[idx],ira_second_power[idx],ira_pc_power[idx],
                    ira_turn[idx],ira_temp_control[idx]]
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
            + common.get_create_table_sql(1, total_fields) + ', ' \
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
    except:
        msg = hdf_name + '`calc 3-dim calc obc data failed'
        common.err(my_log, log_tag, msg)
        return False
    
    
    AGC3=[[0 for j in range(scans)] for i in range(channel)]
    for i in range(0,channel):
        for j in range(0,scans):
            AGC3[i][j]=AGC[j][i]

    for idx in xrange(0, scans):
        batch_data = []
        for chnl in xrange(0, channel):
            # prepare data to batch insert every channels count.
            # data's sequence MUST follow strictly of sql field sequence.
            one_data = [chnl + 1, idx + 1, time_data[idx],gain[chnl][idx]*1000,AGC3[chnl][idx]]
            
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
    
def calc_obc_to_db_iras(hdf_name):
    time_begin = time.time()   
    print 'calc_obc_to_db_iras_2dim now !!!'          
    ret = calc_obc_to_db_iras_2dim(hdf_name)
    print ret
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 2-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        print 'calc_obc_to_db_iras_2dim error !!!'
        return False
    
    
    print 'calc_obc_to_db_iras_3dim now !!!'
    time_begin = time.time()            
    ret = calc_obc_to_db_iras_3dim(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 3-dim calc data to db finished.' \
        + '`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg)    
    if not ret:
        print 'calc_obc_to_db_iras_3dim error !!!'
        return False
    
    return True

def calc_obc_to_db_iras_3dim(hdf_name):
    channel = ins_conf.channels
    
    ira_mirdir_sign_table = common.get_table_name(hdf_name)
    data_table = common.get_table_name(hdf_name)+ '_' + str(channel)
    calc_table = common.get_table_name(hdf_name) + '_CALC' + '_' + str(channel)
    
    total_fields = []
    total_fields.extend(ins_conf.calc_black_cold.values())
    
    
    sql ='create table IF NOT EXISTS %s(id int unsigned primary key AUTO_INCREMENT, channel tinyint unsigned, scln smallint, ymdhms datetime,type  char(32), value int, INDEX channel (channel) )'
    black_204_sql = 'select scln from %s where ira_mirdir_sign=204;'%ira_mirdir_sign_table
    cold_187_sql = 'select scln from %s where ira_mirdir_sign=187;'%ira_mirdir_sign_table

    black_204_scln=[]
    cold_187_scln=[]
    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(calc_table) )
        conn.commit()
        common.debug(my_log, log_tag, hdf_name + '`' + sql%calc_table)  
                  
        cur.execute(sql%calc_table)
        
        cur.execute(black_204_sql)
        black_204_scln = cur.fetchall()
         
        cur.execute(cold_187_sql)
        cold_187_scln = cur.fetchall()
               
        conn.commit()
    except MySQLdb.Error, e:
        msg = hdf_name + '`loading 3-dim obc data to db, Mysql Fatal Error['\
            + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False
    
    insert_sql = 'insert into ' + calc_table + '(channel, scln, ymdhms, type,value' \
        + ") values(%s, %s, %s, %s, %s)"
    
    print black_204_scln
    #calc for black body    
    batch_data = []
    data_sql = 'select * from %s where '%data_table
    for idx, one_scln in enumerate(black_204_scln):
        for channel in range(1,ins_conf.channels+1):
            sql = data_sql + 'scln=' + str(one_scln[0]) +' and channel = ' + str(channel) + ';'
            try:
                cur.execute(sql)
                black_204_data = cur.fetchall()
            except MySQLdb.Error, e:
                msg = hdf_name + '`loading 3-dim calc obc data to db, Mysql Fatal Error['\
                    + str(e.args[0])+']: '+e.args[1]
                common.err(my_log, log_tag, msg)
                return False
            
            batch_data=[]
            for i in xrange(0, 45):
                one_data = [channel, one_scln[0], black_204_data[0][3],'blackbody_view',black_204_data[0][7+i]]
                batch_data.append(tuple(one_data))
            #print batch_data
            try:
                cur.executemany(insert_sql, batch_data)   
            except MySQLdb.Error, e:
                msg = 'inset to db error !!!'
                print msg
                common.err(my_log, log_tag, msg)
                return False
    
    conn.commit()
    
    print cold_187_scln
    #calc for cold sky
    batch_data = []
    data_sql = 'select * from %s where '%data_table
    for idx, one_scln in enumerate(cold_187_scln):
        for channel in range(1,ins_conf.channels+1):
            sql = data_sql + 'scln=' + str(one_scln[0]) +' and channel = ' + str(channel) + ';'
            try:
                cur.execute(sql)
                cold_187_data = cur.fetchall()
            except MySQLdb.Error, e:
                msg = hdf_name + '`select cold sky from db error!!!, Mysql Fatal Error['\
                    + str(e.args[0])+']: '+e.args[1]
                common.err(my_log, log_tag, msg)
                return False
            
            batch_data=[]
            for i in xrange(0, 45):
                one_data = [channel, one_scln[0], cold_187_data[0][3],'coldspace_view',cold_187_data[0][7+i]]
                batch_data.append(tuple(one_data))
            #print batch_data
            try:
                cur.executemany(insert_sql, batch_data)   
            except MySQLdb.Error, e:
                msg = 'inset coldspace_view to db error !!!'
                print msg
                common.err(my_log, log_tag, msg)
                return False
    
    conn.commit()
         
    cur.close()
    conn.close()
    
    return True

      
def iras_calc_for_ira_black_temp(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i][0]*0.0118+17.1373)
        result[i].append(src_data[i][1]*0.0045+16.8960)
        result[i].append(src_data[i][2]*0.0045+16.7762)
        result[i].append(src_data[i][3]*0.0045+16.5199)
    return result        
def iras_calc_for_ira_modulator_temp(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i][0]*0.0118+17.2508)
        result[i].append(src_data[i][1]*0.0044+16.7534)
        result[i].append(src_data[i][2]*0.0044+16.8374)
        result[i].append(src_data[i][3]*0.0044+17.2925)
    return result    
def iras_calc_for_ira_wheel_temp(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i][0]*0.0117+16.6477)
        result[i].append(src_data[i][1]*0.0045+16.9127)
        result[i].append(src_data[i][2]*0.0045+16.9827)
        result[i].append(src_data[i][3]*0.0045+16.2026)
    return result        
def iras_calc_for_ira_parts_temp(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i][0]*0.01+9.8193)
        result[i].append(src_data[i][1]*0.01+9.9549)
        result[i].append(src_data[i][2]*0.0099+9.8359)
        result[i].append(src_data[i][3]*0.01+9.91)
        result[i].append(src_data[i][4]*0.01+9.9053)
    return result
def iras_calc_for_ira_colder_temp(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        v1=src_data[i][0]/819.2
        v2=src_data[i][1]/819.2
        result[i].append(113.16298+39.43225*v1-1.01642*pow(v1,2)+2.52112*pow(v1,3)-1.66843*pow(v1,4)+0.56458*pow(v1,5)-0.09526*pow(v1,6)+0.00637*pow(v1,7))
        result[i].append(74.77236+9.71984*v2-3.4616*pow(v2,2)+3.48808*pow(v2,3)-1.89156*pow(v2,4)+0.56486*pow(v2,5)-0.08725*pow(v2,6)+0.00545*pow(v2,7))
                
    return result        
def iras_calc_for_ira_second_power(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i][0]*0.002448+0.01)
        result[i].append(src_data[i][1]*0.002448+0.01)
        result[i].append(src_data[i][2]*0.001837+0.01)
                
    return result        
def iras_calc_for_ira_pc_power(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i][0]*0.001836+0.01)
        result[i].append(src_data[i][1]*0.001836+0.01)
        result[i].append(src_data[i][2]*0.001836+0.01)
        result[i].append(src_data[i][3]*0.001836+0.01)
    return result    
def iras_calc_for_ira_turn(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i][0]*0.001224+0.01)
        result[i].append(src_data[i][1]*0.001224+0.01)
        result[i].append(src_data[i][2]*0.001224+0.01)
                
    return result    
def iras_calc_for_ira_temp_control(src_data):
    result=[]
    for i in range(0,len(src_data)):
        result.append([])
        result[i].append(src_data[i]*0.001224+0.01)
        
    return result            
        
    
    
    
    
    
    
    
    
def calc_nedn_to_db(hdf_name):
    pool = Pool()
    input = zip([hdf_name]*ins_conf.channels, xrange(1, ins_conf.channels + 1), 
                [sat]*ins_conf.channels, [ins]*ins_conf.channels)
 
    pool_ret = pool.map(do_nedn_one_channel, input)
    pool.close()
    pool.join()
     
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
        nedn = get_iras(hdf_name, channel)

    blackbody_nedn = float(nedn['Hot'])
    coldspace_nedn = float(nedn['Cold'])
#     print '11111111111'
#     print channel
#     print blackbody_nedn
#     print coldspace_nedn
    # insert nedn to db [ ymdh, channel, nedn ]
    filename = os.path.basename(hdf_name) # FY3C_MWTSX_GBAL_L1_20131003_1641
    ymd = filename[ins_conf.pre_year : ins_conf.pre_year + len('20141003')]
    hm = filename[ins_conf.pre_hour : ins_conf.pre_hour + len('2135')]
    ymdh = ymd[0:4] + '-' + ymd[4:6] + '-' + ymd[6:8] + ' ' + hm[0:2] \
           + ':' + hm[2:4] + ':00'
    insert_sql = 'replace into ' + conf.table_setting[sat][ins]['nedn_table'] \
                + " values('" + ymdh + "', " + str(channel) + ', ' \
                + format(blackbody_nedn, '.6f')+ ', ' + format(coldspace_nedn, '.6f') + ')'

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
        msg = hdf_name + '`' + str(channel) + '`open hdf failed' \
            + ', use nedn default value: ' + str(defval)
        common.error(my_log, log_tag, msg)
        return defval
    
    if(NOscan<200):
        msg = hdf_name + '`' + str(channel) + '`scan number[' + str(NOscan) \
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

        dic={'ok':True,'Cold':numpy.mean(CT),'Hot':numpy.mean(HT)}
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


def get_DateCalculate(path,sat,ins,data,channel):
        
        if(sat=='fy3c'and ins=='mwri'):
                return get_mwri_date(path,channel,data)
                
def get_mwri_date(path,channel,data):
        if(data == 'synthesize_temp'):
                return get_mwri_synthesize_temp(path,channel)
                
def get_mwri_synthesize_temp(path,channel):                
        defval=65535
        try:
                hdf = h5.File(path, 'r')     
                obsc=hdf['Calibration/SP_IT_CAL_OBS_COUNT']
                calib=hdf['Calibration/ANTENNA_BT_CALIBRATION_COEF(SCALE+OFFSET)']
                hotc=hdf['Calibration/Hotloadreflector_CSM_Hotload_Temp_Count']
                #hdf.close()
        except:
                print 'erro! file is not available!'
                return defval    
        if(channel==1 or channel==4):
                cl=9
        elif(channel==2):
                cl=10
        elif(channel==3 or channel==6):
                cl=8
        elif(channel==5 or channel==8 or channel==10):
                cl=6
        elif(channel==7 or channel==9):
                cl=5
        else:
                cl=10
        result=[]
        for i in range(0,len(obsc[cl])):
                result.append([])
                result[i].append(obsc[cl][i][channel-1]*calib[i][2*channel-2]+calib[i][2*channel-1]-hotc[i][4])
        return result        
                



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
    print msg
    common.debug(my_log, log_tag, msg)    
    if not ret:
        print 'load obc 2-dim error!!!!!!!!!!!'
        return False
 
    time_begin = time.time()            
    ret = obc_3dim_data_to_db(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`loading obc 3-dim data to db finished.' \
        + '`timeuse=' + timeuse
    print msg
    common.debug(my_log, log_tag, msg)
    if not ret:
        print 'load obc 3-dim error!!!!!!!!!!!!!'
        return False

    time_begin = time.time()            
    ret = calc_nedn_to_db(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`calc nedt finished.`timeuse=' + timeuse
    print msg
    common.debug(my_log, log_tag, msg)    
    if not ret:
        print 'calc nedt error!!!!!!!!!!!'
        return False
    
    print 'calc now !!!'
    time_begin = time.time()         
    ret = calc_obc_to_db(hdf_name)
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    msg = hdf_name + '`calc obc finished.`timeuse=' + timeuse
    common.debug(my_log, log_tag, msg) 
    if not ret:
        print 'calc obc error !!!!'
        return False
    
    return True

# main loop
def main():
    common.wt_file(my_pidfile, str(pid))
    common.info(my_log, log_tag, 'program start. pid=' + str(pid))
    
    signal.signal(signal.SIGTERM, signal_handler)   
    signal.signal(signal.SIGINT, signal_handler)  
    
    while True:
#         print '111111111'
#         calc_obc_to_db_iras_3dim('FY3C_IRASX_GBAL_L1_20140827_1951_OBCXX_MS.HDF')
#         return
    
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
            print ret
            
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


