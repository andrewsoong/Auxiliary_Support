#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""High level configuration."""

__author__ = 'gumeng'

import sys

support_sat_ins = {
    'fy3b': ( 'mwts', 'mwhs'),
    'fy3c': ( 'mwts', 'mwhs', 'mwri', 'iras'),
}
support_nwp = ('t639', 'ncep')

do_hdf = ('hdf_sim_to_db', 'obc_to_db', 'sim', 'mvfile')
do_cron = ('calc_draw_bt', 'calc_draw_oneday_global',
           'calc_draw_lat', 'calc_draw_obc')

# when we get time from day cnt, we should know the time begin.
# like: fy3c is days since 2000-1-1 00:00:0.0
time_begin = {
    'fy3b':'2000-01-01 00:00:00',
    'fy3c':'2000-01-01 00:00:00'
}

data_path = '/assimilation/fymonitor/DATA'

txt_path = data_path + '/OUTPUT/TXT'
# Wu-chun-qiang's simulation system root path.
sim_root_path = data_path

#temp file dir
temp_global = data_path +'/TMP/GLOBAL/'

# /home/fymonitor/DATA/MODEL/T639 or /home/fymonitor/DATA/MODEL/NCEP
sim_original_nwp_path = sim_root_path + '/MODEL' 

sim_original_t639_path = sim_original_nwp_path + '/T639'

# /home/fymonitor/DATA/SAT/NSMC/FY3B/MWTS/2013/10
sim_original_hdf_path = sim_root_path + '/SAT/NSMC' 

sim_input_path = sim_root_path + '/MNTIN'
sim_input_nwp_path = sim_input_path + '/NWP'

# like: /home/fymonitor/DATA/MNTIN/NWP/T639/2013/*.DAT
sim_input_t639_path = sim_input_nwp_path + '/T639' 

sim_output_path = sim_root_path + '/MNTOUT'


    
top_path = '/home/fymonitor/MONITORFY3C'

root_path = top_path + '/py2'
bin_path = root_path + '/bin'
pid_path = root_path + '/pid'
conf_path = root_path + '/conf'
lib_path = root_path + '/lib'
plot_path = root_path + '/plot'

sys.path.append(conf_path)
sys.path.append(lib_path)

log_path = '/home/fymonitor/DATA/LOG'

tmp_path = data_path + '/TMP'

input_path = data_path + '/INPUT'
input_hdf_path = input_path + '/HDF'
# t639 grb2 file patten, like gmf.639.2014041900006.grb2
t639 = '^gmf\.639\.\d{13}\.grb2\.OK$'
input_t639_path = input_path + '/T639'

output_path = data_path + '/OUTPUT'
output_t639_path = output_path + '/T639'

arch_path = data_path + '/ARCH' # archive
arch_hdf_path = arch_path + '/HDF'
arch_t639_path = arch_path + '/T639'

redo_path = data_path + '/REDO'
redo_hdf_path = redo_path + '/HDF'
redo_t639_path = redo_path + '/T639'

img_path = data_path + '/IMG'
img_nsmc = img_path + '/NSMC'
# absolute path like /home/fymonitor/DATA/IMG/NSMC/FY3C/MWTS/BT/ is created 
# by img_nsmc + sat + ins + png_type in your python, not here.
png_type = {'bt': 'BT', 'cold_ang': 'COLDANG', 'earth_ang': 'EARTHANG',
    'global': 'GLOBAL', 'hot_ang': 'HOTANG', 'ins_temp': 'INSTEMP',
    'lat': 'LAT', 'prt': 'PRT', 'cold_hot_cnt':'COLD_HOT_CNT',
    'agc':'AGC','cal_coef':'CAL_COEF',
}

#****add by zhaowl***********
ftpdata_path = data_path + '/FTPDATA'
ftpfiledir = {'639':{
                     'source_dir':ftpdata_path + '/T639/',
                     'dest_dir':input_path + '/T639/'
                     },
                    'MWHS':{
                     'source_dir':ftpdata_path + '/MWHS/',
                     'dest_dir':input_path + '/HDF/'
                     },
                    'MWTS':{
                    'source_dir':ftpdata_path + '/MWTS/',
                    'dest_dir':input_path + '/HDF/'
                    },
                    'IRAS':{
                    'source_dir':ftpdata_path + '/IRAS/',
                    'dest_dir':input_path + '/HDF/'
                    },
                    'MWRI':{
                    'source_dir':ftpdata_path + '/MWRI/',
                    'dest_dir':input_path + '/HDF/'
                    },
                    'GNOS':{
                    'source_dir':ftpdata_path + '/GNOS/',
                    'dest_dir':input_path + '/HDF/'
                    }
                  }  

# ncl scripts for png
draw_bt = plot_path + '/curve_map.ncl'
draw_lat = {'CRTMX': plot_path + '/lat_time_crtm.ncl',
            'RTTOV':  plot_path + '/lat_time_rttov.ncl'}

# /home/fymonitor/MONITOR/tools/procs
# these is just for test.
# bin_trans_t639 = top_path + '/tools/procs/SUBPRC_NWPXT639_GRB2toBIN_1FILE.bash'
# sim_script_path = top_path + '/scripts'
# bin_sim = {
#     'fy3b': sim_script_path + '/Monitor_SDR_FileInvoke_NOCMP_not_exist.bash',
#     'fy3c': sim_script_path + '/Monitor_SDR_FileInvoke_NOCMP.bash',
# }
bin_trans_t639 = '/home/fymonitor/PARAOPER/scripts/procs/SUBPRC_NWPXT639_GRB2toBIN_1FILE.bash'

# one hdf have two sim result: one for rttov, another for crtm
# FY3C_MWTSX_GBAL_L1_20131201_0130_060KM_MS.HDF
# FY3C_MWTSX_GBAL_L1_20131201_0130_060KM_MS_T639_CRTM202.TOVSL1X
# FY3C_MWTSX_GBAL_L1_20131201_0130_060KM_MS_T639_RTTOV101.TOVSL1X
sim_file = {
    't639': {
        'rttov': '_T639_RTTOV101.TOVSL1X',
        'crtm': '_T639_CRTM202.TOVSL1X'
    },
    'ncep': {
        'rttov': '_T639_RTTOV101.TOVSL1X',
        'crtm': '_T639_CRTM202.TOVSL1X',
    },
}

# one hdf have two sim result: one for rttov, another for crtm
# FY3C_MWTSX_GBAL_L1_20131201_0130_060KM_MS.HDF
# FY3C_MWTSX_GBAL_L1_20131201_0130_060KM_MS_FWDBTS_T639_RTTOV101_TOVSL1X.bin
# FY3C_MWTSX_GBAL_L1_20131201_0130_060KM_MS_FWDBTS_T639_CRTM202_TOVSL1X.bin
sim_bin = {
    't639': {
        'rttov': '_FWDBTS_T639_RTTOV101_TOVSL1X.bin',
        'crtm' : '_FWDBTS_T639_CRTM202_TOVSL1X.bin',
    },
    'ncep': {
        'rttov': '_T639_RTTOV101_TOVSL1X',
        'crtm': '_T639_CRTM202_TOVSL1X',
    },
}

sleep_time_conf = conf_path + '/sleep_time.conf'

db_setting = {
    'master': {'ip': '127.0.0.1', 'user':'root', 'pwd':'', 'port':3306},
    'slave': {'ip': '127.0.0.1', 'user':'root', 'pwd':'', 'port':3306},
    'stat_db': 'STAT',
    'info_db': 'INFO',
}
table_setting = {
    'fy3b': {
        'mwts': {
            'data_db': 'FY3C_MWTS_V2', 
            'stat_6h': 'FY3C_MWTS_6H',
            'stat_lat': 'FY3C_MWTS_12H_LAT',
        },
    },
    'fy3c': {
        'mwts': {
            'data_db': 'FY3C_MWTS_V2',
            'nedn_table': 'FY3C_MWTS_NEDN', 
            'stat_6h': 'FY3C_MWTS_BT_6H',
            'stat_6h_5lat': 'FY3C_MWTS_BT_6H_5LAT',
            'stat_12h_5lat': 'FY3C_MWTS_BT_12H_5LAT',
            'info': 'FY3C_MWTS_INFO',
            'daily': 'FY3C_MWTS_DAILY1', 
            'daily_channel': 'FY3C_MWTS_DAILY_13',
            'stat_6h_lat_point':'FY3C_MWTS_LAT_POINT'
        },
        'mwhs': {
            'data_db': 'FY3C_MWHS_V2', 
            'nedn_table': 'FY3C_MWHS_NEDN', 
            'stat_6h': 'FY3C_MWHS_BT_6H',
            'stat_6h_5lat': 'FY3C_MWHS_BT_6H_5LAT',
            'stat_12h_5lat': 'FY3C_MWHS_BT_12H_5LAT',
            'info': 'FY3C_MWHS_INFO',
            'daily': 'FY3C_MWHS_DAILY',
            'daily_channel': 'FY3C_MWHS_DAILY_15',
            'stat_6h_lat_point':'FY3C_MWHS_LAT_POINT'
        },
        'iras': {
            'data_db': 'FY3C_IRAS_V2', 
            'nedn_table': 'FY3C_IRAS_NEDN', 
            'stat_6h': 'FY3C_IRAS_BT_6H',
            'stat_6h_5lat': 'FY3C_IRAS_BT_6H_5LAT',
            'stat_12h_5lat': 'FY3C_IRAS_BT_12H_5LAT',
            'info': 'FY3C_IRAS_INFO',
            'daily': 'FY3C_IRAS_DAILY',
            'daily_channel': 'FY3C_IRAS_DAILY_20',
            'stat_6h_lat_point':'FY3C_IRAS_LAT_POINT'
        },        
        'mwri': {
            'data_db': 'FY3C_MWRI_V2', 
            'nedn_table': 'FY3C_MWRI_NEDN', 
            'stat_6h': 'FY3C_MWRI_BT_6H',
            'stat_6h_5lat': 'FY3C_MWRI_BT_6H_5LAT',
            'stat_12h_5lat': 'FY3C_MWRI_BT_12H_5LAT',
            'info': 'FY3C_MWRI_INFO',
            'daily': 'FY3C_MWRI_DAILY',
            'daily_channel': 'FY3C_MWRI_DAILY_10',
            'stat_6h_lat_point':'FY3C_MWRI_LAT_POINT'
        },  
    },
}

l1b_create_table_sql = 'create table IF NOT EXISTS %s(id int unsigned ' \
            + 'primary key AUTO_INCREMENT, scln smallint, scpt smallint, ' \
            + 'ymdhms datetime, obt_direct tinyint unsigned, '
            
obc_create_table_sql = 'create table IF NOT EXISTS %s(id int unsigned ' \
            + 'primary key AUTO_INCREMENT, scln smallint, ymdhms datetime, '

obc_3dim_create_table_sql = 'create table IF NOT EXISTS %s(id int unsigned ' \
            + 'primary key AUTO_INCREMENT, channel tinyint unsigned, ' \
            + 'scln smallint, ymdhms datetime, '
obc_3dim_create_idx_sql = 'INDEX channel (channel) '

obc_select_prefix_sql = 'select ymdhms, '
obc_3dim_where_sql = ' where channel='

# select count(cold_ang1), min(cold_ang1), max(cold_ang1), STDDEV_POP(cold_ang1) 
# from (
#    select cold_ang1 from FY3C_MWTSX_GBAL_L1_20140731_0119_OBCXX_MS where 
#    cold_ang1!=-99000 union all 
#    select cold_ang1 from FY3C_MWTSX_GBAL_L1_20140731_0300_OBCXX_MS where 
#    cold_ang1!=-99000
# ) as total;
calc_daily_prefix_sql = 'select count(%s), avg(%s), max(%s), min(%s), ' \
                        'STDDEV_POP(%s) from ('
calc_daily_subsql = 'select %s from %s where %s!=%s'
calc_daily_channel_subsql = 'select %s from %s where channel=%s and %s!=%s'
calc_daily_postfix_sql = ') as total'

export_txt = " INTO OUTFILE '%s' FIELDS TERMINATED BY ','"    

drop_table = 'DROP TABLE IF EXISTS %s'


#max_sleep_time=30
max_db_retry = 5        # max db retry times.
max_ps_count = 3        # ps -elf | grep dest_program, should run singleton.
default_sleep_time = 1200 # seconds.
rd_lines_each_time = 500 # info_to_db.php need.
msg_type_info = 'INFO'
msg_type_err = 'ERROR'

# sys cmd
bash = '/bin/bash'
tail = '/usr/bin/tail'
cat = '/bin/cat'
awk = '/bin/awk'
head = '/usr/bin/head'
grep = '/bin/grep'
echo = '/bin/echo'
sort = '/bin/sort'
uniq = '/usr/bin/uniq'
wc = '/usr/bin/wc'
ps = '/bin/ps'
mv = '/bin/mv'
rm = '/bin/rm'
cp = '/bin/cp'
php = '/home/fymonitor/php54/bin/php'
ifconfig = '/sbin/ifconfig'
df = '/bin/df'
touch = '/bin/touch'
kill = '/bin/kill'
xargs = '/usr/bin/xargs'
ping = '/bin/ping'
w = '/usr/bin/w'
iostat = '/usr/bin/iostat'
vmstat = '/usr/bin/vmstat'
timeout = '/usr/bin/timeout'
tar = '/bin/tar'
mkdir = '/bin/mkdir'
#python = '/usr/bin/python'
python = '/home/fymonitor/python27/bin/python'
hostname = '/bin/hostname'
ncl = '/home/fymonitor/ncl/bin/ncl'

draw_ncl = {
       'mwts': {
                'ncl_prog_channel' :{
                     0:{'tmp_png': 'cold_hot_cnt','dir_name':'COLD_HOT_CNT', },
                     1:{'tmp_png': 'agc', 'dir_name':'AGC',},
                     2:{'tmp_png': 'cal_coef', 'dir_name':'CAL_COEF', },
                },
                'ncl_prog_no_channel' :{
                     0:{'tmp_png': 'cold_ang', 'dir_name':'COLDANG', },
                     1:{'tmp_png': 'hot_ang', 'dir_name':'HOTANG',},
                     2:{'tmp_png': 'earth_ang', 'dir_name':'EARTHANG', },
                     3:{'tmp_png': 'ins_temp','dir_name':'INSTEMP', },
                     4:{'tmp_png': 'prt', 'dir_name':'PRT',},
                },       
        },
        'mwhs': {
                'ncl_prog_channel' :{
                     0:{'tmp_png': 'fy3c_mwhs_cal_coef','dir_name':'CAL_COEF', },
                },
                'ncl_prog_no_channel' :{
                     0:{'tmp_png': 'fy3c_mwhs_cold_ang', 'dir_name':'COLDANG', },
                     1:{'tmp_png': 'fy3c_mwhs_hot_ang', 'dir_name':'HOTANG',},
                     2:{'tmp_png': 'fy3c_mwhs_ins_temp','dir_name':'INSTEMP', },
                     3:{'tmp_png': 'fy3c_mwhs_prt', 'dir_name':'PRT',},
                }, 

        },
        'iras': {
                'ncl_prog_channel' :{
                    0:{'tmp_png': 'iras_cal_coef','dir_name':'CAL_COEF', },
                },
                'ncl_prog_no_channel' :{
                     0:{'tmp_png': 'iras_cold_temp', 'dir_name':'CLOD_TEMP', },
                     1:{'tmp_png': 'iras_colder_volt', 'dir_name':'IRA_COLDER2_VOLT', },
                     2:{'tmp_png': 'iras_ira_midir_sign', 'dir_name':'IRA_MIDIR_SIGN', },
                     3:{'tmp_png': 'iras_Angles','dir_name':'ANGLES', },
                     4:{'tmp_png': 'iras_black_temp','dir_name':'BLACK_TEMP', },
                     5:{'tmp_png': 'iras_ira_parts_temp','dir_name':'PARTS_TEMP', },
                     6:{'tmp_png': 'iras_ira_turn','dir_name':'TURN', },
                },                        

        },        
        'mwri': {
                'ncl_prog_channel' :{
                },
                'ncl_prog_no_channel' :{
                     0:{'tmp_png': 'fy3c_mwri_ins_status', 
                        'dir_name':'INS_STATUS', },
                },                        
        },              
}



draw_ncl_new = {
       'mwts': {
                'ncl_prog_channel' :{
                    0:{'tmp_png': 'AGC1', 'dir_name':'AGC1', },
                    1:{'tmp_png': 'AGC2', 'dir_name':'AGC2', },
                    2:{'tmp_png': 'CAL_COEF1', 'dir_name':'CAL_COEF1', },
                    3:{'tmp_png': 'CAL_COEF2', 'dir_name':'CAL_COEF2', },
                    4:{'tmp_png': 'CAL_COEF3', 'dir_name':'CAL_COEF3', },
                    5:{'tmp_png': 'HOT_CNT_AVG', 'dir_name':'HOT_CNT_AVG', },
                    6:{'tmp_png': 'COLD_CNT_AVG', 'dir_name':'COLD_CNT_AVG', },
                },
                'ncl_prog_no_channel' :{
                     0:{'tmp_png': 'COLD_ANG1', 'dir_name':'COLD_ANG1', },
                     1:{'tmp_png': 'COLD_ANG2', 'dir_name':'COLD_ANG2',},
                     2:{'tmp_png': 'HOT_ANG1', 'dir_name':'HOT_ANG1', },
                    3:{'tmp_png': 'HOT_ANG2','dir_name':'HOT_ANG2', },
                    4:{'tmp_png': 'EARTH_ANG1', 'dir_name':'EARTH_ANG1',},
                    5:{'tmp_png': 'EARTH_ANG2', 'dir_name':'EARTH_ANG2',},
                    6:{'tmp_png': 'INS_TEMP', 'dir_name':'INS_TEMP',},
                    7:{'tmp_png': 'PRT1', 'dir_name':'PRT1',},
                    8:{'tmp_png': 'PRT2', 'dir_name':'PRT2',},
                    9:{'tmp_png': 'PRT3', 'dir_name':'PRT3',},
                    10:{'tmp_png': 'PRT4', 'dir_name':'PRT4',},
                    11:{'tmp_png': 'PRT5', 'dir_name':'PRT5',},
                    12:{'tmp_png': 'PRT_AVG', 'dir_name':'PRT_AVG',},
                    13:{'tmp_png': 'COLD_ANG_MINUS', 'dir_name':'COLD_ANG_MINUS',},
                    14:{'tmp_png': 'HOT_ANG_MINUS', 'dir_name':'HOT_ANG_MINUS',},
                    15:{'tmp_png': 'EARTH_ANG_MINUS', 'dir_name':'EARTH_ANG_MINUS',},
                    16:{'tmp_png': 'SCAN_PRD', 'dir_name':'SCAN_PRD',},
                },       
        },
        'mwhs': {
                'ncl_prog_channel' :{
                    0:{'tmp_png': 'CAL_COEF1', 'dir_name':'CAL_COEF1', },
                    1:{'tmp_png': 'CAL_COEF2', 'dir_name':'CAL_COEF2', },
                    2:{'tmp_png': 'CAL_COEF3', 'dir_name':'CAL_COEF3', },
                    3:{'tmp_png': 'AGC', 'dir_name':'AGC', },
                    4:{'tmp_png': 'SPBB1', 'dir_name':'SPBB1', },
                    5:{'tmp_png': 'SPBB2', 'dir_name':'SPBB2', },
                    6:{'tmp_png': 'GAIN', 'dir_name':'GAIN', },
                     
                },
                'ncl_prog_no_channel' :{
                    0:{'tmp_png': 'SCAN_PRD', 'dir_name':'SCAN_PRD', },
                    1:{'tmp_png': 'PRT_AVG1', 'dir_name':'PRT_AVG1', },
                    2:{'tmp_png': 'PRT_AVG2', 'dir_name':'PRT_AVG2', },
                    3:{'tmp_png': 'PIX_VIEW_ANGLE1', 'dir_name':'PIX_VIEW_ANGLE1', },
                    4:{'tmp_png': 'PIX_VIEW_ANGLE2', 'dir_name':'PIX_VIEW_ANGLE2', },
                    5:{'tmp_png': 'ANTENNA_MASK_TEMP1', 'dir_name':'ANTENNA_MASK_TEMP1', },
                    6:{'tmp_png': 'ANTENNA_MASK_TEMP2', 'dir_name':'ANTENNA_MASK_TEMP2', },
                    7:{'tmp_png': 'FET_118_AMP_TEMP', 'dir_name':'FET_118_AMP_TEMP', },
                    8:{'tmp_png': 'FET_183_AMP_TEMP', 'dir_name':'FET_183_AMP_TEMP', },
                    9:{'tmp_png': 'CELL_CONTROL_U', 'dir_name':'CELL_CONTROL_U', },
                    10:{'tmp_png': 'DIGITAL_CONTROL_U', 'dir_name':'DIGITAL_CONTROL_U', },
                    11:{'tmp_png': 'COLD_ANG', 'dir_name':'COLD_ANG', },
                    12:{'tmp_png': 'HOT_ANG', 'dir_name':'HOT_ANG', },
                    13:{'tmp_png': 'INS_TEMP1', 'dir_name':'INS_TEMP1', },
                    14:{'tmp_png': 'INS_TEMP2', 'dir_name':'INS_TEMP2', },
                    15:{'tmp_png': 'MOTOR_TEMP1', 'dir_name':'MOTOR_TEMP1', },
                    16:{'tmp_png': 'MOTOR_TEMP2', 'dir_name':'MOTOR_TEMP2', },
                     
                }, 

        },
        'iras': {
                'ncl_prog_channel' :{
                    0:{'tmp_png': 'BLACKBODY_VIEW', 'dir_name':'BLACKBODY_VIEW', },
                    1:{'tmp_png': 'COLDSPACE_VIEW', 'dir_name':'COLDSPACE_VIEW', },
                    2:{'tmp_png': 'IRA_CALCOEF1', 'dir_name':'IRA_CALCOEF1', },
                    3:{'tmp_png': 'IRA_CALCOEF2', 'dir_name':'IRA_CALCOEF2', },
                    4:{'tmp_png': 'IRA_CALCOEF3', 'dir_name':'IRA_CALCOEF3', },
                },
                'ncl_prog_no_channel' :{
                    0:{'tmp_png': 'IRA_BLACK_TEMP1', 'dir_name':'IRA_BLACK_TEMP1', },
                    1:{'tmp_png': 'IRA_BLACK_TEMP2', 'dir_name':'IRA_BLACK_TEMP2', },
                    2:{'tmp_png': 'IRA_BLACK_TEMP3', 'dir_name':'IRA_BLACK_TEMP3', },
                    3:{'tmp_png': 'IRA_BLACK_TEMP4', 'dir_name':'IRA_BLACK_TEMP4', },
                    4:{'tmp_png': 'IRA_COLDER_TEMP1', 'dir_name':'IRA_COLDER_TEMP1', }, 
                    5:{'tmp_png': 'IRA_COLDER_TEMP2', 'dir_name':'IRA_COLDER_TEMP2', },
                    6:{'tmp_png': 'IRA_MODULATOR_TEMP1', 'dir_name':'IRA_MODULATOR_TEMP1', },
                    7:{'tmp_png': 'IRA_MODULATOR_TEMP2', 'dir_name':'IRA_MODULATOR_TEMP2', },
                    8:{'tmp_png': 'IRA_MODULATOR_TEMP3', 'dir_name':'IRA_MODULATOR_TEMP3', },
                    9:{'tmp_png': 'IRA_MODULATOR_TEMP4', 'dir_name':'IRA_MODULATOR_TEMP4', },
                    10:{'tmp_png': 'IRA_PARTS_TEMP1', 'dir_name':'IRA_PARTS_TEMP1', },
                    11:{'tmp_png': 'IRA_PARTS_TEMP2', 'dir_name':'IRA_PARTS_TEMP2', },
                    12:{'tmp_png': 'IRA_PARTS_TEMP3', 'dir_name':'IRA_PARTS_TEMP3', },
                    13:{'tmp_png': 'IRA_PARTS_TEMP4', 'dir_name':'IRA_PARTS_TEMP4', },
                    14:{'tmp_png': 'IRA_PARTS_TEMP5', 'dir_name':'IRA_PARTS_TEMP5', },
                    15:{'tmp_png': 'IRA_PC_POWER1', 'dir_name':'IRA_PC_POWER1', },
                    16:{'tmp_png': 'IRA_PC_POWER2', 'dir_name':'IRA_PC_POWER2', },
                    17:{'tmp_png': 'IRA_PC_POWER3', 'dir_name':'IRA_PC_POWER3', },
                    18:{'tmp_png': 'IRA_PC_POWER4', 'dir_name':'IRA_PC_POWER4', },
                    19:{'tmp_png': 'IRA_WHEEL_TEMP1', 'dir_name':'IRA_WHEEL_TEMP1', },
                    20:{'tmp_png': 'IRA_WHEEL_TEMP2', 'dir_name':'IRA_WHEEL_TEMP2', },
                    21:{'tmp_png': 'IRA_WHEEL_TEMP3', 'dir_name':'IRA_WHEEL_TEMP3', },  
                    22:{'tmp_png': 'IRA_WHEEL_TEMP4', 'dir_name':'IRA_WHEEL_TEMP4', },  
                    23:{'tmp_png': 'IRA_TURN1', 'dir_name':'IRA_TURN1', },  
                    24:{'tmp_png': 'IRA_TURN2', 'dir_name':'IRA_TURN2', },  
                    25:{'tmp_png': 'IRA_TURN3', 'dir_name':'IRA_TURN3', },  
                    26:{'tmp_png': 'IRA_TEMP_CONTROL', 'dir_name':'IRA_TEMP_CONTROL', },  
                    27:{'tmp_png': 'IRA_SECOND_POWER1', 'dir_name':'IRA_SECOND_POWER1', },  
                    28:{'tmp_png': 'IRA_SECOND_POWER2', 'dir_name':'IRA_SECOND_POWER2', },  
                    29:{'tmp_png': 'IRA_SECOND_POWER3', 'dir_name':'IRA_SECOND_POWER3', },  
                    
                    
                    30:{'tmp_png': 'IRA_BLACK_TEMP', 'dir_name':'IRA_BLACK_TEMP', }, 
                    31:{'tmp_png': 'IRA_MODULATOR_TEMP', 'dir_name':'IRA_MODULATOR_TEMP', },
                    32:{'tmp_png': 'IRA_PC_POWER', 'dir_name':'IRA_PC_POWER', },
                    33:{'tmp_png': 'IRA_WHEEL_TEMP', 'dir_name':'IRA_WHEEL_TEMP', },
                    34:{'tmp_png': 'IRA_PARTS_TEMP', 'dir_name':'IRA_PARTS_TEMP', },
                    35:{'tmp_png': 'IRA_SECOND_POWER', 'dir_name':'IRA_SECOND_POWER', },
                    36:{'tmp_png': 'IRA_COLDER_TEMP', 'dir_name':'IRA_COLDER_TEMP', },               
                },                        

        },        
        'mwri': {
                'ncl_prog_channel' :{
                    0:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS1', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS1', },
                    1:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS2', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS2', },
                    2:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS3', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS3', },
                    3:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS4', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS4', },
                    4:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS5', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS5', },
                    5:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS6', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS6', },
                    6:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS7', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS7', },
                    7:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS8', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS8', },
                    8:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS9', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS9', },
                    9:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS10', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS10', },
                    10:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS11', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS11', },
                    11:{'tmp_png': 'SCAN_WARM_OBSERVATION_COUNTS12', 'dir_name':'SCAN_WARM_OBSERVATION_COUNTS12', },
                    12:{'tmp_png': 'SCAN_COLD_OBSERVATION_COUNTS1', 'dir_name':'SCAN_COLD_OBSERVATION_COUNTS1', },
                    13:{'tmp_png': 'SCAN_COLD_OBSERVATION_COUNTS2', 'dir_name':'SCAN_COLD_OBSERVATION_COUNTS2', },
                    14:{'tmp_png': 'SCAN_COLD_OBSERVATION_COUNTS3', 'dir_name':'SCAN_COLD_OBSERVATION_COUNTS3', },
                    15:{'tmp_png': 'SCAN_COLD_OBSERVATION_COUNTS4', 'dir_name':'SCAN_COLD_OBSERVATION_COUNTS4', },
                    16:{'tmp_png': 'SCAN_COLD_OBSERVATION_COUNTS5', 'dir_name':'SCAN_COLD_OBSERVATION_COUNTS5', },
                    17:{'tmp_png': 'SCAN_COLD_OBSERVATION_COUNTS6', 'dir_name':'SCAN_COLD_OBSERVATION_COUNTS6', },
                    18:{'tmp_png': 'AGC_CONTROL_VOLT_COUNT', 'dir_name':'AGC_CONTROL_VOLT_COUNT', },
                    19:{'tmp_png': 'ANTENNA_BT_CALIBRATION_SCALE', 'dir_name':'ANTENNA_BT_CALIBRATION_SCALE', },
                    20:{'tmp_png': 'ANTENNA_BT_CALIBRATION_OFFSET', 'dir_name':'ANTENNA_BT_CALIBRATION_OFFSET', },
                    21:{'tmp_png': 'SYNTHETIC_BRIGHT_TEMP', 'dir_name':'SYNTHETIC_BRIGHT_TEMP', },
                    
                },
                'ncl_prog_no_channel' :{
                    
                    0:{'tmp_png': 'SCAN_PERIOD', 'dir_name':'SCAN_PERIOD', },
                    1:{'tmp_png': 'RX_TEMP_COUNT1', 'dir_name':'RX_TEMP_COUNT1', },
                    2:{'tmp_png': 'RX_TEMP_COUNT2', 'dir_name':'RX_TEMP_COUNT2', },
                    3:{'tmp_png': 'RX_TEMP_COUNT3', 'dir_name':'RX_TEMP_COUNT3', },
                    4:{'tmp_png': 'RX_TEMP_COUNT4', 'dir_name':'RX_TEMP_COUNT4', },
                    5:{'tmp_png': 'RX_TEMP_COUNT5', 'dir_name':'RX_TEMP_COUNT5', },
                    6:{'tmp_png': 'ANTENNA_TEMPERATURE_AB1', 'dir_name':'ANTENNA_TEMPERATURE_AB1', },
                    7:{'tmp_png': 'ANTENNA_TEMPERATURE_AB2', 'dir_name':'ANTENNA_TEMPERATURE_AB2', },
                    8:{'tmp_png': 'HORN_TEMPERATURE_EXCEPT_37GHZ1', 'dir_name':'HORN_TEMPERATURE_EXCEPT_37GHZ1', },
                    9:{'tmp_png': 'HORN_TEMPERATURE_EXCEPT_37GHZ2', 'dir_name':'HORN_TEMPERATURE_EXCEPT_37GHZ2', },
                    10:{'tmp_png': 'HORN_TEMPERATURE_EXCEPT_37GHZ3', 'dir_name':'HORN_TEMPERATURE_EXCEPT_37GHZ3', },
                    11:{'tmp_png': 'HORN_TEMPERATURE_EXCEPT_37GHZ4', 'dir_name':'HORN_TEMPERATURE_EXCEPT_37GHZ4', },
                    12:{'tmp_png': 'HOT_LOAD_REFLECTOR1', 'dir_name':'HOT_LOAD_REFLECTOR1', },
                    13:{'tmp_png': 'HOT_LOAD_REFLECTOR2', 'dir_name':'HOT_LOAD_REFLECTOR2', },
                    14:{'tmp_png': 'COLD_SKY_MIRROR_TEMPERATURE1', 'dir_name':'COLD_SKY_MIRROR_TEMPERATURE1', },
                    15:{'tmp_png': 'COLD_SKY_MIRROR_TEMPERATURE2', 'dir_name':'COLD_SKY_MIRROR_TEMPERATURE2', },
                    16:{'tmp_png': 'HOT_LOAD_PHYSICAL_TEMPERATURE1', 'dir_name':'HOT_LOAD_PHYSICAL_TEMPERATURE1', },
                    17:{'tmp_png': 'HOT_LOAD_PHYSICAL_TEMPERATURE2', 'dir_name':'HOT_LOAD_PHYSICAL_TEMPERATURE2', },
                    18:{'tmp_png': 'HOT_LOAD_PHYSICAL_TEMPERATURE3', 'dir_name':'HOT_LOAD_PHYSICAL_TEMPERATURE3', },
                    19:{'tmp_png': 'HOT_LOAD_PHYSICAL_TEMPERATURE4', 'dir_name':'HOT_LOAD_PHYSICAL_TEMPERATURE4', },
                    20:{'tmp_png': 'HOT_LOAD_PHYSICAL_TEMPERATURE5', 'dir_name':'HOT_LOAD_PHYSICAL_TEMPERATURE5', },
                    
                    
                    
                  
                },                        
        },              
}


