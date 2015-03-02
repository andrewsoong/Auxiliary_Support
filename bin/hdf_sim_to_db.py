#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""All day run program, put L1B hdf and simulation bin data to db.

Usage:
    hdf_sim_to_db.py --sat=mysat --ins=myins --nwp=t639

Arguments:
    sat: the satellite you want to calc
    ins: the insatrument you want to calc
    nwp: t639 or ncep

"""

__author__ = 'gumeng'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# work flow:
#  get sorted hdf data from input_hdf_path with .OK
#  for each hdf file:
#      check validation, all input files are ok
#      construct mysql table record
#      read hdf and sim result, create array of mysql table record.
#      wt array to tmpfile
#      mysql: load data infile 'tmpfile'
#      insert basic info to STAT db
#         
# /usr/bin/python /home/fymonitor/MONITORFY3C/py2/bin/hdf_sim_to_db.py --sat=fy3c 
# --ins=mwts --nwp=t639 >> /home/fymonitor/DATA/LOG/hdf_sim_to_db.py.log 2>&1
#                         
# date          author    changes
# 2014-07-11    gumeng    add hdf len .VS. sim.bin.len check 
# 2014-06-23    gumeng    bug fix: sim_data['time'][one_scan] --> 
#                          [one_scan*ins_conf.pixels + one_pixel]
# 2014-05-28    gumeng    bug fix: len(rttov) --> str(len(rttov) )
# 2014-03-31    gumeng    create

import os
import sys
import time
import signal
import commands
import warnings
import h5py as h5
import MySQLdb

warnings.filterwarnings('ignore', category = MySQLdb.Warning)

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
conf = __import__('conf')
common = __import__('common')
docopt = __import__('docopt')

# set read-only global variables.

arguments = docopt.docopt(__doc__)
sat = arguments['--sat'].lower()
ins = arguments['--ins'].lower()
nwp = arguments['--nwp'].lower()

ins_conf_file = sat.upper() + '_' + ins.upper() + '_CONF'
ins_conf = __import__(ins_conf_file)

if sat not in conf.support_sat_ins or ins not in conf.support_sat_ins[sat] \
    or nwp not in conf.support_nwp:
    print 'sat or ins or nwp setting is NOT found in conf.py'
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
    msg = 'recv signal ' + str(signum) + '. exit now.'
    common.info(my_log, log_tag, msg)

    if os.path.isfile(my_pidfile):
        os.remove(my_pidfile)
    
    sys.exit(0)

# get data from sim's bin data, we just get want we need.
def get_data_from_sim(hdf_name):
    data = {'ok': False, 'rttov': None, 'crtm': None, 'time': None}
    
    if common.check_sim_bin(sat, ins, nwp, hdf_name) is False:
        return data
    
    file = common.get_sim_bin(sat, ins, nwp, hdf_name)

    rttov = common.get_record_from_sim(file['rttov'], ins_conf.sim_fmt)
    crtm = common.get_record_from_sim(file['crtm'], ins_conf.sim_fmt)
 
    if len(rttov) != len(crtm):
        msg = hdf_name + '`sim record size is not equal. rttov size is ' \
            + str(len(rttov) ) + ', while crtm size is ' + str(len(crtm) )
        common.err(my_log, log_tag, msg)        
        return data
    elif len(rttov) == 0:
        msg = hdf_name + '`sim record size is zero. rttov size is ' \
            + str(len(rttov) ) + ', crtm size is ' + str(len(crtm) )
        common.err(my_log, log_tag, msg)        
        return data

    data['time'] = common.get_time_data(rttov, ins_conf.sim_time_idx)
    data['rttov'] = common.get_sim_data(rttov, ins_conf.rttov_to_db, 
                                        ins_conf.channels)
    data['crtm'] = common.get_sim_data(crtm, ins_conf.crtm_to_db, 
                                        ins_conf.channels)
    data['ok'] = True
    return data

# if hdf bt data is NOT the same size with sim bt data, then simulation 
# or hdf bt data maybe wrong.
def check_hdf_sim_len(hdf_name, hdf_data, sim_data):
    scans_in_sim = int( len(sim_data) / ins_conf.pixels )
    scans_in_hdf = int( len(hdf_data[ins_conf.bt_idx][0]) )
    
    attr = common.get_attr_from_hdf(hdf_name, ins_conf)
    if attr['ok'] is False:
        scans_attr = False
    else:
        scans_attr = attr['scans']
        
    if scans_in_sim != scans_in_hdf:
        msg = hdf_name + '`scan number is not equal. scans in sim bin is ' \
            + str(scans_in_sim) + ', while scans in hdf is ' + str(scans_in_hdf)
        common.err(my_log, log_tag, msg)
        
        return False
    elif scans_attr != scans_in_sim:
        msg = hdf_name + '`scan number attribute[' + str(scans_attr)+'] in ' \
            + 'hdf error. scans in sim bin is ' + str(scans_in_sim) \
            + ', and scans in hdf is ' + str(scans_in_hdf)
        common.err(my_log, log_tag, msg)
        return False
    else:
        return True 

# get scans, pixels, sds data from hdf.
# sds dtype is db_dtype for db.
def get_data_from_hdf(hdf_name):
    data = {'ok': False, 'scans': 0, 'pixels': 0, 'hdf_data': None}

    attr = common.get_attr_from_hdf(hdf_name, ins_conf)
    if not attr['ok']:
        common.err(my_log, log_tag, attr['msg'])
        return data

    data['scans'] = attr['scans']
    data['pixels'] = attr['pixels']
        
    total_sds = (ins_conf.lat_sds_to_db, ins_conf.sds_to_db)
    
    hdf = common.get_hdf_l1b(hdf_name, ins_conf)
    hdf_fd = {'l0': None, 'l1b': None, 'l1b_obc': None, 'l1b_geo': None}
    if hdf['l0']:
        hdf_fd['l0'] = h5.File(hdf['l0'], 'r')
    if hdf['l1b']:
        hdf_fd['l1b'] = h5.File(hdf['l1b'], 'r')
    if hdf['l1b_obc']:
        hdf_fd['l1b_obc'] = h5.File(hdf['l1b_obc'], 'r')
    if hdf['l1b_geo']:
        hdf_fd['l1b_geo'] = h5.File(hdf['l1b_geo'], 'r')

    #get all data in HDF
    total_data = []
    
    for sds_setting in total_sds:
        sds_len = len(sds_setting)
        for i in range(1, sds_len + 1):
            sds = sds_setting[i]['sds']
            src = sds_setting[i]['src']
            
            if sds not in hdf_fd[src]:
                msg = hdf_name + '`' + src + ' file, sds ' + sds + ' not exist.'
                common.err(my_log, log_tag, msg)
                return data
            
            dataset = hdf_fd[src][sds]
            # h5yp: To retrieve the contents of a scalar dataset, can use the 
            # same syntax as in NumPy: result = dset[()]. In other words, index 
            # into the dataset using an empty tuple.
            sds_data = common.trans_data_for_db(dataset[()], sds_setting[i])
            total_data.extend([sds_data])
        
    if hdf['l0']:
        hdf_fd['l0'].close()
    if hdf['l1b']:
        hdf_fd['l1b'].close()
    if hdf['l1b_obc']:
        hdf_fd['l1b_obc'].close()
    if hdf['l1b_geo']:
        hdf_fd['l1b_geo'].close()

    data['hdf_data'] = total_data
    data['ok'] = True

    return data        

# Insert hdf and sim data to mysql table.
# Warning: if the talbe exist, just truncate and load again.
# table name like: FY3C_MWTSX_GBAL_L1_20140210_0250_033KM_MS_T639, which is the 
# basename of one HDF file.
def hdf_sim_to_db(hdf_name):
    hdf_data = get_data_from_hdf(hdf_name)
    if not hdf_data['ok']:
        return False

    scans = hdf_data['scans']
    pixels = hdf_data['pixels']
    hdf_value = hdf_data['hdf_data']
    channel = ins_conf.channels

    sim_data = get_data_from_sim(hdf_name)
    if not sim_data['ok']:
        return False
    
    if check_hdf_sim_len(hdf_name, hdf_value, sim_data['rttov']) is False:
        return False
    
    hdf_fields = []
    hdf_fields.extend(ins_conf.lat_sds_to_db.values())
    hdf_fields.extend(ins_conf.sds_to_db.values())
    
    table = common.get_table_name(hdf_name) + '_' + nwp.upper()

    # wt to tmp file for db load.
    tmpfile = conf.tmp_path + '/' + my_tag + '.' + table + '.' \
            + common.utc_YmdHMS() + '.txt'
            
    fp = open(tmpfile, 'w')
                
    time_begin = time.time()
    data_cnt = 0
    #fillval=65535
    #tmp=[fillval for j in range(scans)]
    for one_scan in xrange(0, scans):
        for one_pixel in xrange(0, pixels):
            data_cnt += 1
            one_data = [one_scan + 1, one_pixel + 1,
                        sim_data['time'][one_scan*ins_conf.pixels + one_pixel]]
            for idx, one_sds in enumerate(hdf_fields):
                if one_sds['dims'] == 3:
                    # 3-dim data is always channel*scans*pixel fmt.
                    for i in xrange(0, one_sds['use_ch']):
                        one_data.extend([hdf_value[idx][i][one_scan][one_pixel]])
                elif one_sds['dims'] == 2:
                    #if len(hdf_value[idx][one_scan])<pixels:
                    #    for i in range(0,pixels-len(hdf_value[idx][one_scan])):
                    #        hdf_value[idx][one_scan].append(tmp)
                    one_data.extend([ hdf_value[idx][one_scan][one_pixel] ])
                    
                elif one_sds['dims'] == 1:
                    one_data.extend([ hdf_value[idx][one_scan] ])
                else:
                    pass
                    
            one_data.extend(sim_data['rttov'][one_scan*pixels + one_pixel])
            one_data.extend(sim_data['crtm'][one_scan*pixels + one_pixel])
            txt_data = map(common.data_to_str, one_data)
            fp.write("%s\n"%','.join(txt_data))

    fp.close()
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    common.debug(my_log, log_tag, hdf_name + '`' + 'wt ' + str(data_cnt) \
                + ' records to ' + tmpfile + '`timeues=' + timeuse)

    # create load sql tpl, like:
    # load data infile '/home/fymonitor/tmp/data.txt' replace into table
    # FY3C_MWTSX_GBAL_L1_20140210_0250_033KM_MS_T639 fields terminated by ',' 
    # lines terminated by '\n' (scln, scpt, ymdhms, crtm_nwp_end_coef);
    total_fields = [ field for field in hdf_fields]
    total_fields.extend(ins_conf.rttov_to_db.values())
    total_fields.extend(ins_conf.crtm_to_db.values())
    showtype = False
    field_sql = common.get_sql_dict(channel, total_fields, showtype)
    create_sql = conf.l1b_create_table_sql \
               + common.get_create_table_sql(channel, total_fields) + ')'    
    load_sql = "load data infile '" + tmpfile + "' replace into table " \
             + table + " fields terminated by ',' lines terminated by '\\n' " \
             + '(scln, scpt, ymdhms, ' + field_sql['field'] + ')'
    common.debug(my_log, log_tag, hdf_name + '`' + create_sql%table)
    common.debug(my_log, log_tag, hdf_name + '`' + load_sql)            

    time_begin = time.time()
    try:
        conn = MySQLdb.connect(host = conf.db_setting['master']['ip'],
                               user = conf.db_setting['master']['user'],
                               passwd = conf.db_setting['master']['pwd'],
                               port = conf.db_setting['master']['port'])
        cur = conn.cursor()
        conn.select_db(conf.table_setting[sat][ins]['data_db'])
        cur.execute(conf.drop_table%(table) )
        conn.commit()
        cur.execute(create_sql%(table))
        cur.execute(load_sql)
        conn.commit()
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = hdf_name + '`Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
        common.err(my_log, log_tag, msg)
        return False    
    finally:
        common.rm_file(tmpfile)
        
    time_end = time.time()
    timeuse = str(round(time_end - time_begin, 2))
    common.debug(my_log, log_tag, hdf_name + '`' + 'load ' + str(data_cnt) \
                + ' records from ' + tmpfile + ' to db`timeues=' + timeuse)
    return True    

# calc orbit direction for one hdf data, then updata db
def calc_orbit_direction(hdf_name):
    table = get_table_name(hdf_name)

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])        
        cur=conn.cursor()
        conn.select_db(my_setting['data_db'])
        sql = 'select scln, ' + ins_conf.lat_sds_to_db[1]['db_field'] \
            + ' from ' + table + ' where scpt=1 order by scln '
        cur.execute(sql)
        data = cur.fetchall()
    except MySQLdb.Error, e:
        msg = 'Error: ' + hdf_name + " Mysql Fatal Error[" + str(e.args[0]) \
            + ']: ' + e.args[1] + '. Do not calc orbit direction for this file'
        common.err(my_log, log_tag, msg)
        return
    
    scans = len(data)
    if scans <= 1:
        msg = 'Error: ' + hdf_name + '. only ' + str(scans) \
            + ' scans, can NOT calc orbit direction.'
        common.err(my_log, log_tag, msg)
        return
        
    cur_direction = 1
    previous_direction = 1
    
    # set previous_direction value.
    first_lat = data[0][1]
    second_lat = data[1][1]
    if (first_lat - second_lat) > 0 :
        # descend
        previous_direction = 2
    elif (first_lat - second_lat) < 0 :
        # ascend
        previous_direction = 1
    else:
        previous_direction = 1
    
    sql_update = 'update ' + table + ' set obt_direct=%s where ' \
               + 'scln between %s and %s'
    
    begin_scan = 1
    for one_scan in xrange(begin_scan, scans - 1):
        cur_lat = data[one_scan][1]
        next_lat = data[one_scan + 1][1]
        
        if (cur_lat - next_lat) > 0 :
            # descend
            cur_direction = 2
        elif (cur_lat - next_lat) < 0 :
            # ascend
            cur_direction = 1
        else:
            cur_direction = previous_direction
    
        if cur_direction == previous_direction:
            # still in the same direction, do not update db
            continue
        
        # direction changed, we should update db till cur scan
        sql = sql_update%(previous_direction, begin_scan, one_scan)
        try:
            cur.execute(sql)
        except MySQLdb.Error, e:
            msg = 'Error: ' + hdf_name + " Mysql Fatal Error[" + str(e.args[0]) \
                + ']: ' + e.args[1] + '. Do not calc orbit direction for this file'
            common.err(my_log, log_tag, msg)
            return
        # update flag
        previous_direction = cur_direction
        begin_scan = one_scan + 1
    
    # update the last scan
    sql_last = sql_update%(previous_direction, begin_scan, scans)
    try:
        cur.execute(sql_last)
    except MySQLdb.Error, e:
        msg = 'Error: ' + hdf_name + " Mysql Fatal Error[" + str(e.args[0]) \
            + ']: ' + e.args[1] + '. Do not calc orbit direction for this file'
        common.err(my_log, log_tag, msg)
        return
    
    conn.commit()
    cur.close()
    conn.close()

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
        print conf.input_hdf_path
        hdf = common.get_files(conf.input_hdf_path, ins_conf.sim_ok, \
                               '.SIM.OK', 'hdf')
        
        for one_hdf in hdf:
	    print one_hdf
            cur_time = common.utc_nowtime()
            common.wt_file(my_alivefile, cur_time)
            
            # check *L1B.OK file, avoid redo.
            l1b_ok_file = one_hdf + '.L1B.OK'
            if os.path.isfile(l1b_ok_file) or not os.path.isfile(one_hdf+'.OK'):
                continue
            
#             if common.hdf_l1b_ok(one_hdf, ins_conf) is False:
#                 print 'hdf_l1b_ok false'
#             else:
#                 print 'hdf_l1b_ok true'
#                 
#             if common.check_sim_bin(sat, ins, nwp, one_hdf) is False:
#                 print 'check_sim_bin false'
#             else:
#                 print 'check_sim_bin true'

            # check validation: are L0, l1b, obc, geo files ok?
            if common.hdf_l1b_ok(one_hdf, ins_conf) is False \
                or common.check_sim_bin(sat, ins, nwp, one_hdf) is False:
                msg = one_hdf + '`L1 hdf or sim bin files ' \
                    + 'not find. mv to redo folder, please redo manually.'
                common.err(my_log, log_tag, msg)
                common.mv_hdf(one_hdf + '.SIM.OK', conf.redo_hdf_path)
                common.mv_hdf(one_hdf + '.OK', conf.redo_hdf_path)
                common.mv_hdf(one_hdf, conf.redo_hdf_path)
                continue

            msg = one_hdf + '`loading to db now'
            common.info(my_log, log_tag, msg)
        
            time_begin = time.time()
            print one_hdf
            ret = hdf_sim_to_db(one_hdf)
            time_end = time.time()
            timeuse = str(round(time_end - time_begin, 2))

            if ret:
                common.wt_file(l1b_ok_file, l1b_ok_file)
                msg = one_hdf + '`SUCC`load hdf and sim data to db finished.' \
                    + '`timeuse=' + timeuse
                common.info(my_log, log_tag, msg)
                # arch hdf
                info = common.get_filename_year_mon(one_hdf, ins_conf)       
                arch = conf.arch_hdf_path + '/' + sat.upper() + '/' \
                     + ins.upper() + '/' + info[1] + '/' + info[2]
                common.mv_hdf(l1b_ok_file, arch)
                common.mv_hdf(one_hdf + '.SIM.OK', arch)
                common.mv_hdf(one_hdf + '.OK', arch)
                common.mv_hdf(one_hdf, arch)
            else:
                msg = one_hdf + '`FAILED`please redo manually.' \
                    + '`timeuse=' + timeuse
                common.err(my_log, log_tag, msg)
                # 80% errors is file-system error. we need re-sim, del SIM.OK
                common.rm_file(one_hdf + '.SIM.OK')
                common.mv_hdf(one_hdf + '.OK', conf.redo_hdf_path)
                common.mv_hdf(one_hdf, conf.redo_hdf_path)
            
        my_sleep_time = common.get_sleep_time()
        for i in range(0, my_sleep_time, 1):
            time.sleep(1)

if __name__ == '__main__':
    main()


