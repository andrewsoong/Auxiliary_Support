#/usr/bin/env python
# -*- coding: utf-8 -*-

"""Instrument setting.
"""

__author__ = 'gumeng'

# hdf l1b file regx patten
L1B = '^FY3C_MWHSX_GBAL_L1_\d{8}_\d{4}_\d{3}KM_MS\.HDF\.OK$'

# hdf string prefix patten about L1B
L0 = None
L1B_OBC = None #'FY3C_MWTSX_GBAL_L1_%s_%s_OBCXX_MS.HDF'
L1B_GEO = None

# hdf obc file regx patten
OBC = '^FY3C_MWHSX_GBAL_L1_\d{8}_\d{4}_OBCXX_MS\.HDF\.OK$'

sim_ok = '^FY3C_MWHSX_GBAL_L1_\d{8}_\d{4}_\d{3}KM_MS\.HDF\.SIM\.OK$'
pre_year = len('FY3C_MWHSX_GBAL_L1_')
pre_mon = len('FY3C_MWHSX_GBAL_L1_2013')
pre_hour = len('FY3C_MWHSX_GBAL_L1_20131201_')

# channels for your instrument
channels = 15

# pixels for your instrument
pixels = 98

# scans attribute name in HDF
scans_name = 'Count_scnlines'


#data_db = 'FY3C_MWTS_V2'

# src: l0, l1b, l1b_obc, l1b_geo.
# dims: 3 means that is like channels*scans*pixel, 
#    so, we should create table like: obs_bt1, obs_bt2, obs_bt3, obs_bt4
#    and, 'rank': {'x': 'scan', 'y': 'pixel', 'z': 'channel'}
#    means: 3-dim data fmt is scan*pixel*channel
# db_dtype: datatype in db. we just save %.4f when float: 1.23456 --> 1.2345
#        but float-->int is recommended!
# factor: be used ONLY if hdf_datatype != db_datatype, such as:
#    int = int( float * 100 )
# idx MUST start from 1.

# latitude sds, we can calc ascend or descend orbit.
lat_sds_to_db = {
    1: {'src': 'l1b', 
        'sds': 'Geolocation/Latitude', 'hdf_dtype': 'float', 'dims': 2, 
        'db_field': 'lat', 'db_dtype': 'int', 'factor': 100}                 
}

# l1b sds.
sds_to_db = {
    1: {'src': 'l1b', 
        'sds': 'Geolocation/Longitude', 'hdf_dtype': 'float', 'dims': 2, 
        'db_field': 'lon', 'db_dtype': 'int', 'factor': 100},
    2: {'src': 'l1b', 
        'sds': 'Geolocation/SensorZenith', 'hdf_dtype': 'smallint', 'dims': 2, 
        'db_field': 'sen_zen', 'db_dtype': 'smallint', 'factor': 1},
    3: {'src': 'l1b', 
        'sds': 'Geolocation/SensorAzimuth', 'hdf_dtype': 'smallint', 'dims': 2, 
        'db_field': 'sen_az', 'db_dtype': 'smallint', 'factor': 1},
    4: {'src': 'l1b', 
        'sds': 'Geolocation/SolarZenith', 'hdf_dtype': 'smallint', 'dims': 2, 
        'db_field': 'solar_zen', 'db_dtype': 'smallint', 'factor': 1},
    5: {'src': 'l1b', 
        'sds': 'Geolocation/SolarAzimuth', 'hdf_dtype': 'smallint', 'dims': 2, 
        'db_field': 'solar_az', 'db_dtype': 'smallint', 'factor': 1},
    6: {'src': 'l1b', 
        'sds': 'Data/LandSeaMask', 'hdf_dtype': 'tinyint unsigned', 
        'dims': 2, 'db_field': 'landsea', 'db_dtype': 'tinyint unsigned', 
        'factor': 1},
    7: {'src': 'l1b', 
        'sds': 'Data/DEM', 'hdf_dtype': 'smallint', 'dims': 2, 
        'db_field': 'dem', 'db_dtype': 'smallint', 'factor': 1},
    8: {'src': 'l1b',
        'sds': 'Data/Earth_Obs_BT', 'hdf_dtype': 'float','dims': 3,
        'db_field': 'obs_bt', 'db_dtype': 'int', 'factor': 100,
        'rank': {'x': 'channel', 'y': 'scan', 'z': 'pixel'},
        'use_ch':15},
    
}

# idx of 'sds': 'Data/Earth_Obs_BT'
bt_idx = len(lat_sds_to_db) + len(sds_to_db) - 1

# time sds, we can get ymd-hms directly from daycnt and mscnt.
time_sds = {
    'daycnt': {'src': 'l1b_obc', 'sds': 'Geolocation/Scnlin_daycnt', 
               'hdf_dtype': 'int', 'dims': 2, 'db_dtype': 'int'},
    'mscnt': {'src': 'l1b_obc', 'sds': 'Geolocation/Scnlin_mscnt', 
              'hdf_dtype': 'int', 'dims': 2, 'db_dtype': 'int'},
}

# simulation field in db. one for rttov, another for crtm.
# idx: for sim bin data, start from 0.
# dims: 3 means that is channels*scans*pixel, we should create table like: 
#    sim_bt1, sim_bt2, ..., sim_bt[channels]
#    and, the data idx should increase continuely to (idx + channels) 
# sim_dtype is data type for sim bin data.
# if default value found in sim's result bin data, we should 
# trans to db default value instead.
# if sim_default is None, we do NOT check sim default value.
rttov_to_db = {
    1: {'db_field': 'rttov_bt', 
        'dims': 3, 'db_dtype': 'int',
        'idx': 23, 'sim_dtype': 'int', 'factor': 1,
        'sim_default': -2147483648, 'db_default': -2147483648},
    2: {'db_field': 'rttov_nwp_begin_t', 
        'dims': 1, 'db_dtype': 'double',
        'idx': 40, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None},               
    3: {'db_field': 'rttov_nwp_begin_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 42, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    4: {'db_field': 'rttov_nwp_end_t', 
        'dims': 1, 'db_dtype': 'double',
        'idx': 41, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    5: {'db_field': 'rttov_nwp_end_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 43, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
}
crtm_to_db = {
    1: {'db_field': 'crtm_bt', 
        'dims': 3, 'db_dtype': 'int',
        'idx': 23, 'sim_dtype': 'int', 'factor': 1,
        'sim_default': -2147483648, 'db_default': -2147483648},
    2: {'db_field': 'crtm_nwp_begin_t', 
        'dims': 1, 'db_dtype': 'double',
        'idx': 40, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None},               
    3: {'db_field': 'crtm_nwp_begin_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 42, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    4: {'db_field': 'crtm_nwp_end_t', 
        'dims': 1, 'db_dtype': 'double',
        'idx': 41, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    5: {'db_field': 'crtm_nwp_end_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 43, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
}

# sim result bin fmt. both rttov and crtm fmt MUST NOT be difference.
# one record = 38 int[4] + 2 nwp file time [double,8] + 2 nwp coef [float,4]
# sim_fmt = '=' + 'i'*38 + 'd'*2 + 'f'*2
sim_fmt = '=' + 'i'*40 + 'd'*2 + 'f'*2

# time data is int fmt. ONLY used in sim bin data.
sim_time_idx = {
    'idx_year': 7, 'idx_mon': 8, 'idx_day': 9, 'idx_hour':10, 
    'idx_min':11, 'idx_sec': False,
}

# obc 2 dims hdf sds setting
# scans*column = datasize
obc_to_db = {
    1: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Calibration/Inst_Temp', 'hdf_dtype': 'float', 'columns': 2, 
        'db_field': 'ins_temp', 'db_dtype': 'int unsigned','factor': 100},
    2: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Calibration/PRT_Tavg', 'hdf_dtype': 'float', 'columns': 2, 
        'db_field': 'prt_avg', 'db_dtype': 'int unsigned','factor': 1000},
    3: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Calibration/Space_View_Ang', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'cold_ang', 'db_dtype': 'smallint unsigned','factor': 0.01},
    4: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Calibration/Black_Body_View_Ang', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'hot_ang', 'db_dtype': 'smallint unsigned','factor': 0.01},               
    5: {'src': 'l1b', 'dims': 2, 'fill_value': 0,
        'sds': 'Data/Pixel_View_Angle', 'hdf_dtype': 'int', 'columns': 2,
        'db_field': 'pixviewangle', 'db_dtype': 'int', 'factor': 1},
}

sds_name = {
    
    
    1: {'name': 'cold_ang',},
    2: {'name': 'hot_ang',},
    3: {'name': 'ins_temp1',},
    4: {'name': 'ins_temp2',},
    5: {'name': 'prt_avg1',},
    6: {'name': 'prt_avg2',},
    7: {'name': 'pixviewangle1',},
    8: {'name': 'pixviewangle2',},
    9: {'name': 'digital_control_u',},
    10: {'name': 'cell_control_u',},
    11: {'name': 'motor_temp_1',},
    12: {'name': 'motor_temp_2',},
    13: {'name': 'antenna_mask_temp_1',},
    14: {'name': 'antenna_mask_temp_2',},
    15: {'name': 'fet_118_amp_temp',},
    16: {'name': 'fet_183_amp_temp',},
    17: {'name': 'scan_prd',},
    
    
    
    
             
}

calc_to_db = {
          
    1: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/Digital_Control_U', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'digital_control_u', 'db_dtype': 'int unsigned','factor': 1000},
    2: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/Cell_Control_U', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'cell_control_u', 'db_dtype': 'int unsigned','factor': 1000},
    3: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/Motor_Temp_1', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'motor_temp_1', 'db_dtype': 'int unsigned','factor': 1000}, 
    4: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/Motor_Temp_2', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'motor_temp_2', 'db_dtype': 'int unsigned','factor': 1000},
    5: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/Antenna_Mask_Temp_1', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'antenna_mask_temp_1', 'db_dtype': 'int unsigned','factor': 1000},
    6: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/Antenna_Mask_Temp_2', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'antenna_mask_temp_2', 'db_dtype': 'int unsigned','factor': 1000},
    7: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/FET_118_Amp_Temp', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'fet_118_amp_temp', 'db_dtype': 'int unsigned','factor': 1000},
    8: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/FET_183_Amp_Temp', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'fet_183_amp_temp', 'db_dtype': 'int unsigned','factor': 1000},
    9: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Data/scan_prd', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'scan_prd', 'db_dtype': 'int unsigned','factor': 1},
}

# obc 3 dims hdf sds setting
# channels*scans*column = datasize
# need_avg: do not load total data to db, just load avg.
#     eg: mwts.Cold_Sky_Count_Avg have 8 point per scan, but we just need
#        avg(Cold_Sky_Count_Avg[5...7])
# avg_idx: tuple of Cold_Sky_Count_Avg idx for avg. start from 0.
obc_3dim_to_db = {
    1: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Calibration/Cal_Coefficient', 'hdf_dtype': 'int', 'columns': 3, 
        'db_field': 'cal_coef', 'db_dtype': 'int','factor': 1,
        'need_avg': False, 'avg_idx': () ,
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':15},
}

calc_3dim_to_db = {
    1: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Calibration/Gain', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'gain', 'db_dtype': 'int','factor': 1000,
        'need_avg': False, 'avg_idx': () ,
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':15},
    2: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Calibration/AGC', 'hdf_dtype': 'smallint unsigned', 'columns': 1, 
        'db_field': 'agc', 'db_dtype': 'smallint unsigned','factor': 1,
        'need_avg': False, 'avg_idx': () ,
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':15},
    3: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Calibration/SPBB_DN_Avg', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'SPBB1', 'db_dtype': 'int','factor': 1000,
        'need_avg': False, 'avg_idx': () ,
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':15},
    4: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Calibration/SPBB_DN_Avg', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'SPBB2', 'db_dtype': 'int','factor': 1000,
        'need_avg': False, 'avg_idx': () ,
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':15},         
            
}

# calc for bt avg and stdp... ...

# functions in sql fmt. this is just for ONE chanel !!! sql like:
# select count(*), avg(obs_bt3/100 - crtm_bt3/100), STDDEV_POP(obs_bt3/100 - 
#         crtm_bt3/100) from ( select obs_bt3, crtm_bt3 from 
#                        FY3B_MWTSX_GBAL_L1_20131110_0220_060KM_MS union all 
#                             select obs_bt3, crtm_bt3 from 
#                        FY3B_MWTSX_GBAL_L1_20131110_1048_060KM_MS ) as total
# here we split this sql to the following slice, in which,
# 'param_dim_cnt': 2 means 2 %s used in 'param_fmt': 'obs_bt%s - sim_bt%s'
# 'abs_value' is abs(obs-sim)<$value for each channel
sql_crtm_bt = {
    'prefix': 'select count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - crtm_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - crtm_bt%s/100', 'param_fmt_cnt': 2},
    },
    'sub_sql': {
        'prefix': 'select ',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'crtm_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'crtm_bt', 'fields_dims': 3, 
                'not_equal': '-2147483648'},
            2: {'type': 'abs', 'param_fmt': 'obs_bt%s - crtm_bt%s', 
                'param_fmt_cnt': 2, 'fields_dims': 3,
                'abs_value' : {
                    1: '5000', 2: '5000', 3: '5000', 4: '5000', 5: '5000', 
                    6: '5000', 7: '5000', 8: '5000', 9: '5000', 10: '5000', 
                    11: '5000', 12: '5000', 13: '5000',14:'5000',15:'5000'} },
        },
    },
    'group_by': '',
}

sql_rttov_bt = {
    'prefix': 'select count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - rttov_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - rttov_bt%s/100', 'param_fmt_cnt': 2},
    },
    'sub_sql': {
        'prefix': 'select ',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'rttov_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'rttov_bt', 'fields_dims': 3, 
                'not_equal': '-2147483648'},
            2: {'type': 'abs', 'param_fmt': 'obs_bt%s - rttov_bt%s', 
                'param_fmt_cnt': 2, 'fields_dims': 3,
                'abs_value' : {
                    1: '5000', 2: '5000', 3: '5000', 4: '5000', 5: '5000', 
                    6: '5000', 7: '5000', 8: '5000', 9: '5000', 10: '5000', 
                    11: '5000', 12: '5000', 13: '5000',14:'5000',15:'5000'} },
        },
    },
    'group_by': '',
}

# calc for bt avg and stdp, every 5 or 10 lat... ...

sql_crtm_bt_lat = {
    'prefix': 'select count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - crtm_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - crtm_bt%s/100', 'param_fmt_cnt': 2},
    },
    'sub_sql': {
        'prefix': 'select ',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'crtm_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'crtm_bt', 'fields_dims': 3, 
                'not_equal': '-2147483648'},
            2: {'type': 'abs', 'param_fmt': 'obs_bt%s - crtm_bt%s', 
                'param_fmt_cnt': 2, 'fields_dims': 3,
                'abs_value' : {
                    1: '5000', 2: '5000', 3: '5000', 4: '5000', 5: '5000', 
                    6: '5000', 7: '5000', 8: '5000', 9: '5000', 10: '5000', 
                    11: '5000', 12: '5000', 13: '5000',14: '5000', 15: '5000'} },
            3: {'type': 'min_max', 'fmt': 'lat', 'fields_dims': 2},
        },
    },            
    'group_by': '',
}

sql_rttov_bt_lat = {
    'prefix': 'select count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - rttov_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - rttov_bt%s/100', 'param_fmt_cnt': 2},
    },
    'sub_sql': {
        'prefix': 'select ',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'rttov_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'rttov_bt', 'fields_dims': 3, 
                'not_equal': '-2147483648'},
            2: {'type': 'abs', 'param_fmt': 'obs_bt%s - rttov_bt%s', 
                'param_fmt_cnt': 2, 'fields_dims': 3,
                'abs_value' : {
                    1: '5000', 2: '5000', 3: '5000', 4: '5000', 5: '5000', 
                    6: '5000', 7: '5000', 8: '5000', 9: '5000', 10: '5000', 
                    11: '5000', 12: '5000', 13: '5000',14: '5000', 15: '5000'} },
            3: {'type': 'min_max', 'fmt': 'lat', 'fields_dims': 2},
        },
    },            
    'group_by': '',
}

#the factor export out Global HDF
global_hdf_factor = {
                     'lat': 0.01,'lon': 0.01,'obs_bt': 0.01,
                     'sim_bt_rttov': 0.01,'diff_rttov': 0.01,
                     'sim_bt_crtm': 0.01, 'diff_crtm': 0.01, 
                     }




sql_rttov_by_point_realtime = {
    'prefix': 'select count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - rttov_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - rttov_bt%s/100', 'param_fmt_cnt': 2},
        3: {'name': 'sqrt(sum(pow(', 'param_dims': 'point', 
            'param_fmt': 'obs_bt%s/100 - rttov_bt%s/100', 'param_fmt_cnt': 2},
            
    },
    'sub_sql': {
        'prefix': 'select ',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'rttov_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'rttov_bt', 'fields_dims': 3, 
                'not_equal': '-2147483648'},
            2: {'type': 'abs', 'param_fmt': 'obs_bt%s - rttov_bt%s', 
                'param_fmt_cnt': 2, 'fields_dims': 3,
                'abs_value' : {
                    1: '2000', 2: '2000', 3: '2000', 4: '2000', 5: '2000', 
                    6: '2000', 7: '2000', 8: '2000', 9: '2000', 10: '2000', 
                    11: '2000', 12: '2000', 13: '2000', 14: '2000', 15: '2000'} },
            3: {'type': 'point', 'fmt': 'scpt', 'fields_dims': 1},
            4: {'type': 'realtime_start', 'param_fmt': 'ymdhms','fields_dims': 1},
            5: {'type': 'realtime_end', 'param_fmt': 'ymdhms','fields_dims': 1},
    
        },
    },            
    'group_by': '',
}




sql_crtm_bt_point_realtime = {
    'prefix': 'select count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - crtm_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 
            'param_fmt': 'obs_bt%s/100 - crtm_bt%s/100', 'param_fmt_cnt': 2},
        3: {'name': 'sqrt(sum(pow(', 'param_dims': 'point', 
            'param_fmt': 'obs_bt%s/100 - crtm_bt%s/100', 'param_fmt_cnt': 2},

    },
    'sub_sql': {
        'prefix': 'select ',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'crtm_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'crtm_bt', 'fields_dims': 3, 
                'not_equal': '-2147483648'},
            2: {'type': 'abs', 'param_fmt': 'obs_bt%s - crtm_bt%s', 
                'param_fmt_cnt': 2, 'fields_dims': 3,
                'abs_value' : {
                    1: '2000', 2: '2000', 3: '2000', 4: '2000', 5: '2000', 
                    6: '2000', 7: '2000', 8: '2000', 9: '2000', 10: '2000', 
                    11: '2000', 12: '2000', 13: '2000', 14: '2000', 15: '2000'} },
            #3: {'type': 'min_max', 'fmt': 'lat', 'fields_dims': 2},
            3: {'type': 'point', 'fmt': 'scpt', 'fields_dims': 1},
            4: {'type': 'realtime_start', 'param_fmt': 'ymdhms','fields_dims': 1},
            5: {'type': 'realtime_end', 'param_fmt': 'ymdhms','fields_dims': 1},
        },
    },            
    'group_by': '',
}




