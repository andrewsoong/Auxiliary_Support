#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""All day run program, put OBC hdf data to db.

Usage:
    del_table.py --sat=fy3c --ins=mwts --deltable=no

Arguments:
    sat: the satellite you want to calc
    ins: the insatrument you want to calc
    deltable: whether delet the table from the DB,yes delete,no not delete
              
"""

import os
import sys
import time
import numpy
import signal
import commands
import warnings
import MySQLdb
import h5py as h5
import shutil
from datetime import datetime
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 

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

now_time = datetime.now()
ymd = now_time.strftime('%Y%m')

ymd = '2013102'
print ymd

bk_path = '/assimilation/fymonitor/DATA/ARCH/DB/FY3C/MWTS/'+ymd[0:4]+'/'

def time_to_arr(data):
    return [data[0:4], data[5:7], data[8:10], \
              data[11:13], data[14:16],data[17:19]]


def turnhdf_033kmdata(table):
    filename = bk_path + table +'.HDF'
    arr = numpy.loadtxt(bk_path + table + '.txt',dtype='str',delimiter=',')
    
    hdf = h5.File(filename, 'w')
    
    id = hdf.create_dataset("id",data=arr[: ,0].astype(numpy.int))
    scln = hdf.create_dataset("scln",data=arr[: ,1].astype(numpy.int))
    scpt = hdf.create_dataset("scpt",data=arr[: ,2].astype(numpy.int))
    
    ymdh_arr = numpy.array(map(time_to_arr, arr[:, 3]) )
    hdf.create_dataset("time", data = ymdh_arr.astype(numpy.int32))
    #obt_direct = hdf.create_dataset("obt_direct",data=arr[: ,4].astype(numpy.str))
    
    lat = hdf.create_dataset("lat",data=arr[: ,5].astype(numpy.int))
    lon = hdf.create_dataset("lon",data=arr[: ,6].astype(numpy.int))
    
    sen_zen = hdf.create_dataset("sen_zen",data=arr[: ,7].astype(numpy.int))
    sen_az = hdf.create_dataset("sen_az",data=arr[: ,8].astype(numpy.int))
    solar_zen = hdf.create_dataset("solar_zen",data=arr[: ,9].astype(numpy.int))
    solar_az = hdf.create_dataset("solar_az",data=arr[: ,10].astype(numpy.int))
    landsea = hdf.create_dataset("landsea",data=arr[: ,11].astype(numpy.int))
    
    dem = hdf.create_dataset("dem",data=arr[: ,12].astype(numpy.int))
    obs_bt1 = hdf.create_dataset("obs_bt1",data=arr[: ,13].astype(numpy.int))
    obs_bt2 = hdf.create_dataset("obs_bt2",data=arr[: ,14].astype(numpy.int))
    obs_bt3 = hdf.create_dataset("obs_bt3",data=arr[: ,15].astype(numpy.int))
    obs_bt4 = hdf.create_dataset("obs_bt4",data=arr[: ,16].astype(numpy.int))
    obs_bt5 = hdf.create_dataset("obs_bt5",data=arr[: ,17].astype(numpy.int))
    obs_bt6 = hdf.create_dataset("obs_bt6",data=arr[: ,18].astype(numpy.int))
    obs_bt7 = hdf.create_dataset("obs_bt7",data=arr[: ,19].astype(numpy.int))
    obs_bt8 = hdf.create_dataset("obs_bt8",data=arr[: ,20].astype(numpy.int))
    obs_bt9 = hdf.create_dataset("obs_bt9",data=arr[: ,21].astype(numpy.int))
    obs_bt10 = hdf.create_dataset("obs_bt10",data=arr[: ,22].astype(numpy.int))
    obs_bt11 = hdf.create_dataset("obs_bt11",data=arr[: ,23].astype(numpy.int))
    obs_bt12 = hdf.create_dataset("obs_bt12",data=arr[: ,24].astype(numpy.int))
    obs_bt13 = hdf.create_dataset("obs_bt13",data=arr[: ,25].astype(numpy.int))

    rttov_bt1 = hdf.create_dataset("rttov_bt1",data=arr[: ,26].astype(numpy.int))
    rttov_bt2 = hdf.create_dataset("rttov_bt2",data=arr[: ,27].astype(numpy.int))
    rttov_bt3 = hdf.create_dataset("rttov_bt3",data=arr[: ,28].astype(numpy.int))
    rttov_bt4 = hdf.create_dataset("rttov_bt4",data=arr[: ,29].astype(numpy.int))
    rttov_bt5 = hdf.create_dataset("rttov_bt5",data=arr[: ,30].astype(numpy.int))
    rttov_bt6 = hdf.create_dataset("rttov_bt6",data=arr[: ,31].astype(numpy.int))
    rttov_bt7 = hdf.create_dataset("rttov_bt7",data=arr[: ,32].astype(numpy.int))
    rttov_bt8 = hdf.create_dataset("rttov_bt8",data=arr[: ,33].astype(numpy.int))
    rttov_bt9 = hdf.create_dataset("rttov_bt9",data=arr[: ,34].astype(numpy.int))
    rttov_bt10 = hdf.create_dataset("rttov_bt10",data=arr[: ,35].astype(numpy.int))
    rttov_bt11 = hdf.create_dataset("rttov_bt11",data=arr[: ,36].astype(numpy.int))
    rttov_bt12 = hdf.create_dataset("rttov_bt12",data=arr[: ,37].astype(numpy.int))
    rttov_bt13 = hdf.create_dataset("rttov_bt13",data=arr[: ,38].astype(numpy.int))

    rttov_nwp_begin_t = hdf.create_dataset("rttov_nwp_begin_t",data=arr[: ,39].astype(numpy.float32))
    rttov_nwp_begin_coef = hdf.create_dataset("rttov_nwp_begin_coef",data=arr[: ,40].astype(numpy.float32))

    rttov_nwp_end_t = hdf.create_dataset("rttov_nwp_end_t",data=arr[: ,41].astype(numpy.float32))
    rttov_nwp_end_coef = hdf.create_dataset("rttov_nwp_end_coef",data=arr[: ,42].astype(numpy.float32))
    
    crtm_bt1 = hdf.create_dataset("crtm_bt1",data=arr[: ,43].astype(numpy.int))
    crtm_bt2 = hdf.create_dataset("crtm_bt2",data=arr[: ,44].astype(numpy.int))
    crtm_bt3 = hdf.create_dataset("crtm_bt3",data=arr[: ,45].astype(numpy.int))
    crtm_bt4 = hdf.create_dataset("crtm_bt4",data=arr[: ,46].astype(numpy.int))
    crtm_bt5 = hdf.create_dataset("crtm_bt5",data=arr[: ,47].astype(numpy.int))
    crtm_bt6 = hdf.create_dataset("crtm_bt6",data=arr[: ,48].astype(numpy.int))
    crtm_bt7 = hdf.create_dataset("crtm_bt7",data=arr[: ,49].astype(numpy.int))
    crtm_bt8 = hdf.create_dataset("crtm_bt8",data=arr[: ,50].astype(numpy.int))
    crtm_bt9 = hdf.create_dataset("crtm_bt9",data=arr[: ,51].astype(numpy.int))
    crtm_bt10 = hdf.create_dataset("crtm_bt10",data=arr[: ,52].astype(numpy.int))
    crtm_bt11 = hdf.create_dataset("crtm_bt11",data=arr[: ,53].astype(numpy.int))
    crtm_bt12 = hdf.create_dataset("crtm_bt12",data=arr[: ,54].astype(numpy.int))
    crtm_bt13 = hdf.create_dataset("crtm_bt13",data=arr[: ,55].astype(numpy.int))
   
    crtm_nwp_begin_t = hdf.create_dataset("crtm_nwp_begin_t",data=arr[: ,56].astype(numpy.float32))
    crtm_nwp_begin_coef = hdf.create_dataset("crtm_nwp_begin_coef",data=arr[: ,57].astype(numpy.float32))
   
    crtm_nwp_end_t = hdf.create_dataset("crtm_nwp_end_t",data=arr[: ,58].astype(numpy.float32))
    crtm_nwp_end_coef = hdf.create_dataset("crtm_nwp_end_coef",data=arr[: ,59].astype(numpy.float32))
   
    hdf.close()
    os.remove(bk_path + table + '.txt')
    return True

    
    
def turnhdf_obc_ms13(table):
    filename = bk_path + table +'.HDF'
    arr = numpy.loadtxt(bk_path + table + '.txt',dtype='str',delimiter=',')
    
    hdf = h5.File(filename, 'w')
    
    id = hdf.create_dataset("id",data=arr[: ,0].astype(numpy.int))
    channel = hdf.create_dataset("channel",data=arr[: ,1].astype(numpy.int))
    scln = hdf.create_dataset("scln",data=arr[: ,2].astype(numpy.int))
    
    ymdh_arr = numpy.array(map(time_to_arr, arr[:, 3]) )
    hdf.create_dataset("time", data = ymdh_arr.astype(numpy.int32))
    
    cold_cnt_avg = hdf.create_dataset("cold_cnt_avg",data=arr[: ,4].astype(numpy.int))
    hot_cnt_avg = hdf.create_dataset("hot_cnt_avg",data=arr[: ,5].astype(numpy.int))
    agc1 = hdf.create_dataset("agc1",data=arr[: ,6].astype(numpy.int))
    agc2 = hdf.create_dataset("agc2",data=arr[: ,7].astype(numpy.int))
    cal_coef1 = hdf.create_dataset("cal_coef1",data=arr[: ,8].astype(numpy.int))
    cal_coef2 = hdf.create_dataset("cal_coef2",data=arr[: ,9].astype(numpy.int))
    cal_coef3 = hdf.create_dataset("cal_coef3",data=arr[: ,10].astype(numpy.int))
    
    hdf.close()
    os.remove(bk_path + table + '.txt')
    return True
    

def turnhdf_obc_ms(table):
    filename = bk_path + table +'.HDF'
    arr = numpy.loadtxt(bk_path + table + '.txt',dtype='str',delimiter=',')
    
    hdf = h5.File(filename, 'w')
    
    id = hdf.create_dataset("id",data=arr[: ,0].astype(numpy.int))
    scln = hdf.create_dataset("scln",data=arr[: ,1].astype(numpy.int))
    
    ymdh_arr = numpy.array(map(time_to_arr, arr[:, 2]) )
    hdf.create_dataset("time", data = ymdh_arr.astype(numpy.int32))
    ins_temp = hdf.create_dataset("ins_temp",data=arr[: ,3].astype(numpy.int))
    prt1 = hdf.create_dataset("prt1",data=arr[: ,4].astype(numpy.int))
    prt2 = hdf.create_dataset("prt2",data=arr[: ,5].astype(numpy.int))
    prt3 = hdf.create_dataset("prt3",data=arr[: ,6].astype(numpy.int))
    prt4 = hdf.create_dataset("prt4",data=arr[: ,7].astype(numpy.int))
    prt5 = hdf.create_dataset("prt5",data=arr[: ,8].astype(numpy.int))
    prt_avg = hdf.create_dataset("prt_avg",data=arr[: ,9].astype(numpy.int))
    cold_ang1 = hdf.create_dataset("cold_ang1",data=arr[: ,10].astype(numpy.int))
    cold_ang2 = hdf.create_dataset("cold_ang2",data=arr[: ,11].astype(numpy.int))

    hot_ang1 = hdf.create_dataset("hot_ang1",data=arr[: ,12].astype(numpy.int))
    hot_ang2 = hdf.create_dataset("hot_ang2",data=arr[: ,13].astype(numpy.int))

    earth_ang1 = hdf.create_dataset("earth_ang1",data=arr[: ,14].astype(numpy.int))
    earth_ang2 = hdf.create_dataset("earth_ang2",data=arr[: ,15].astype(numpy.int))
    hdf.close()
    os.remove(bk_path + table + '.txt')
    return True

def main():

    try:
        conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                             user=conf.db_setting['master']['user'],
                             passwd=conf.db_setting['master']['pwd'], 
                             port=conf.db_setting['master']['port'])
        cur=conn.cursor()
        conn.select_db('FY3C_MWTS_V2')
        
        cmd = 'show tables like ' + '\'%' + 'FY3C_MWTSX_GBAL_L1_' + ymd + '%\';'
        cur.execute(cmd)
        print cmd
        all_tables = cur.fetchall()
        for table in all_tables:
            print table[0]
            export_cmd = 'select * from ' + table[0] + ' INTO OUTFILE ' + '\'' \
                    + bk_path + table[0] +'.txt' + '\'' + ' FIELDS TERMINATED BY ' + '\'' + ',' + '\''
            print export_cmd
            cur.execute(export_cmd)
            if 'MS_T639' in table[0]:
                turnhdf_033kmdata(table[0])
            elif 'OBCXX_MS_13' in table[0]:
                turnhdf_obc_ms13(table[0])
            elif 'OBCXX_MS' in table[0]:
                turnhdf_obc_ms(table[0])
            else:
                print 'table name is error!!!!'
            
            print arguments['--deltable']  
              
            if arguments['--deltable'] == 'yes':
                cmd = 'drop table ' + table[0] +' ;'
                print cmd
                cur.execute(cmd)
                print 'drop table ' + table[0] + ' Sucess!!!!'
                
        cur.close()
        conn.close()
    except MySQLdb.Error, e:
        msg = 'Mysql Fatal Error[' + str(e.args[0])+']: '+e.args[1] 
        print msg
        sys.exit(3)


if __name__ == '__main__':
    main()
