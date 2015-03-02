#/usr/bin/env python
# -*- coding: utf-8 -*-

"""Instrument setting.
"""

__author__ = 'gumeng'

# hdf l1b file regx patten
L1B = '^FY3C_IRAS_GBAL_L1_\d{8}_\d{4}_\d{3}KM_MS\.HDF\.OK$'

# hdf string prefix patten about L1B
L0 = None
L1B_OBC = None #'FY3C_MWTSX_GBAL_L1_%s_%s_OBCXX_MS.HDF'
L1B_GEO = None

# hdf obc file regx patten
OBC = '^FY3C_IRASX_GBAL_L1_\d{8}_\d{4}_OBCXX_MS\.HDF\.OK$'

sim_ok = '^FY3C_IRASX_GBAL_L1_\d{8}_\d{4}_\d{3}KM_MS\.HDF\.SIM\.OK$'
pre_year = len('FY3C_IRASX_GBAL_L1_')
pre_mon = len('FY3C_IRASX_GBAL_L1_2013')
pre_hour = len('FY3C_IRASX_GBAL_L1_20131201_')

#channels for your instrument
channels = 20 # 20 = 26 - 6, the last 6 channels are NOT used at all!

#pixels for your instrument
# for MWRI, the pixels are NOT uniq, so we should get pixels num from HDF.attri.
pixels = 56

# scans attribute name in HDF
scans_name = 'Number Of Scans'

# src: l0, l1b, l1b_obc, l1b_geo.
# dims: 3 means that is channels*scans*pixel, 
#    so, we should create table like: obs_bt1, obs_bt2, obs_bt3, obs_bt4
# hdf_dtype: datatype in HDF.
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
        'sds': 'Geolocation/LandSeaMask', 'hdf_dtype': 'tinyint unsigned', 
        'dims': 2, 'db_field': 'landsea', 'db_dtype': 'tinyint unsigned', 
        'factor': 1},
    7: {'src': 'l1b', 
        'sds': 'Geolocation/DEM', 'hdf_dtype': 'smallint', 'dims': 2, 
        'db_field': 'dem', 'db_dtype': 'smallint', 'factor': 1},
    8: {'src': 'l1b',                                     
        'sds': 'Data/IRAS_TB', 'hdf_dtype': 'float','dims': 3,
        'db_field': 'obs_bt', 'db_dtype': 'int', 'factor': 100,
        'rank': {'x': 'channel', 'y': 'scan', 'z': 'pixel'},
        'use_ch':20},
}
bt_idx = len(lat_sds_to_db) + len(sds_to_db) - 1

#'rank': 'channel_first', 
# time sds, we can get ymd-hms directly from daycnt and mscnt.
time_sds = {
    'daycnt': {'src': 'l1b_obc', 'sds': 'Data/Scnlin_daycnt', 
               'hdf_dtype': 'int', 'dims': 2, 'db_dtype': 'int'},
    'mscnt': {'src': 'l1b_obc', 'sds': 'Data/Scnlin_mscnt', 
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
        'idx': 29, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None},               
    3: {'db_field': 'rttov_nwp_begin_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 31, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    4: {'db_field': 'rttov_nwp_end_t', 
        'dims': 1, 'db_dtype': 'double',
        'idx': 30, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    5: {'db_field': 'rttov_nwp_end_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 32, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
}
crtm_to_db = {
    1: {'db_field': 'crtm_bt', 
        'dims': 3, 'db_dtype': 'int',
        'idx': 23, 'sim_dtype': 'int', 'factor': 1,
        'sim_default': -2147483648, 'db_default': -2147483648},
    2: {'db_field': 'crtm_nwp_begin_t', 
        'dims': 1, 'db_dtype': 'double',
        'idx': 29, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None},               
    3: {'db_field': 'crtm_nwp_begin_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 31, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    4: {'db_field': 'crtm_nwp_end_t', 
        'dims': 1, 'db_dtype': 'double',
        'idx': 30, 'sim_dtype': 'double', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
    5: {'db_field': 'crtm_nwp_end_coef', 
        'dims': 1, 'db_dtype': 'float',
        'idx': 32, 'sim_dtype': 'float', 'factor': 1, 
        'sim_default': None, 'db_default': None}, 
}

# sim result bin fmt. both rttov and crtm fmt MUST NOT be difference.
# one record = 38 int[4] + 2 nwp file time [double,8] + 2 nwp coef [float,4]
sim_fmt = '=' + 'i'*51 + 'd'*2 + 'f'*2

# time data is int fmt. ONLY used in sim bin data.
sim_time_idx = {
    'idx_year': 7, 'idx_mon': 8, 'idx_day': 9, 'idx_hour':10, 
    'idx_min':11, 'idx_sec': False,
}

calc_black_cold = {
    1: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_black_temp', 'hdf_dtype': 'int', 'columns': 1, 
        'db_field': 'blackbody_view', 'db_dtype': 'int','factor': 1},
    2: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_colder_temp', 'hdf_dtype': 'int', 'columns': 1, 
        'db_field': 'coldspace_view', 'db_dtype': 'int','factor': 1},              
        }
# obc 2 dims hdf sds setting
# scans*column = datasize
obc_to_db = {           
    1: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_black_temp', 'hdf_dtype': 'int', 'columns': 4, 
        'db_field': 'ira_black_temp', 'db_dtype': 'int','factor': 1},
    2: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_colder_temp', 'hdf_dtype': 'int', 'columns': 2, 
        'db_field': 'ira_colder_temp', 'db_dtype': 'int','factor': 1},
    3: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Geolocation/Angles', 'hdf_dtype': 'int', 'columns': 4, 
        'db_field': 'Angles', 'db_dtype': 'int','factor': 1},
    4: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_modulator_temp', 'hdf_dtype': 'int', 'columns': 4, 
        'db_field': 'ira_modulator_temp', 'db_dtype': 'int','factor': 1},
    5: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_wheel_temp', 'hdf_dtype': 'int', 'columns': 4, 
        'db_field': 'ira_wheel_temp', 'db_dtype': 'int','factor': 1},
    6: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_parts_temp', 'hdf_dtype': 'int', 'columns': 5, 
        'db_field': 'ira_parts_temp', 'db_dtype': 'int','factor': 1},
    7: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_second_power', 'hdf_dtype': 'int', 'columns': 3, 
        'db_field': 'ira_second_power', 'db_dtype': 'int','factor': 1},
    8: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_pc_power', 'hdf_dtype': 'int', 'columns': 4, 
        'db_field': 'ira_pc_power', 'db_dtype': 'int','factor': 1},
    9: {'src': 'l1b_obc', 'dims': 2, 'fill_value': 0,
        'sds': 'Telemetry/ira_turn', 'hdf_dtype': 'int', 'columns': 3, 
        'db_field': 'ira_turn', 'db_dtype': 'int','factor': 1},
    10: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_colder2_volt', 'hdf_dtype': 'int', 'columns': 1, 
        'db_field': 'ira_colder2_volt', 'db_dtype': 'int','factor': 1},
    11: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_mirdir_sign', 'hdf_dtype': 'int', 'columns': 1, 
        'db_field': 'ira_mirdir_sign', 'db_dtype': 'int','factor': 1},     
    12: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_temp_control', 'hdf_dtype': 'int', 'columns': 1, 
        'db_field': 'ira_temp_control', 'db_dtype': 'int','factor': 1},       
}

# obc 3 dims hdf sds setting
# channels*scans*column = datasize
# need_avg: do not load total data to db, just load avg.
#     eg: mwts.Cold_Sky_Count_Avg have 8 point per scan, but we just need
#        avg(Cold_Sky_Count_Avg[5...7])
# avg_idx: tuple of Cold_Sky_Count_Avg idx for avg. start from 0.
obc_3dim_to_db = {
    1: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Data/ira_calcoef', 'hdf_dtype': 'float', 'columns': 3, 
        'db_field': 'ira_calcoef', 'db_dtype': 'float','factor': 1,
        'need_avg': False, 'avg_idx': (),  
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':20 },
    2: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Data/IRAS_DN', 'hdf_dtype': 'float', 'columns': 56, 
        'db_field': 'IRAS_DN', 'db_dtype': 'float','factor': 1,
        'need_avg': False, 'avg_idx': (),
        'rank': {'x': 'channel', 'y': 'scan', 'z': 'pixel'},
        'use_ch':20 },
}

obc_3dim_output_db = {
    1: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Data/ira_calcoef', 'hdf_dtype': 'float', 'columns': 3, 
        'db_field': 'ira_calcoef', 'db_dtype': 'float','factor': 1,
        'need_avg': False, 'avg_idx': (),  
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':20 },
}

obc_3dim_to_calc = {
    1: {'src': 'l1b_obc', 'dims': 3, 'fill_value': 0,
        'sds': 'Data/ira_calcoef', 'hdf_dtype': 'float', 'columns': 3, 
        'db_field': 'ira_calcoef', 'db_dtype': 'float','factor': 1,
        'need_avg': False, 'avg_idx': (),  
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':20 },
}

calc_3dim_to_db = {
    1: {'src': 'l1b_obc', 'dims': 1, 'fill_value': -999999,
        'sds': 'blackbody_view', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'blackbody_view', 'db_dtype': 'float','factor': 1,
        'need_avg': False, 'avg_idx': (),  
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':20 },  
    2: {'src': 'l1b_obc', 'dims': 1, 'fill_value': -999999,
        'sds': 'coldspace_view', 'hdf_dtype': 'float', 'columns': 1, 
        'db_field': 'coldspace_view', 'db_dtype': 'float','factor': 1,
        'need_avg': False, 'avg_idx': (),  
        'rank': {'x': 'scan', 'y': 'channel', 'z': 'pixel'},
        'use_ch':20 },                     
}


calc_to_db = {
    1: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_black_temp', 'hdf_dtype': 'int', 'columns': 4,
        'db_field': 'ira_black_temp', 'db_dtype': 'int','factor': 1},
    2: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_modulator_temp', 'hdf_dtype': 'int', 'columns': 4,
        'db_field': 'ira_modulator_temp', 'db_dtype': 'int','factor': 1},
    3: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_wheel_temp', 'hdf_dtype': 'int', 'columns': 4,
        'db_field': 'ira_wheel_temp', 'db_dtype': 'int','factor': 1},
    4: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_parts_temp', 'hdf_dtype': 'int', 'columns': 5,
        'db_field': 'ira_parts_temp', 'db_dtype': 'int','factor': 1},
    5: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_colder_temp', 'hdf_dtype': 'int', 'columns': 2,
        'db_field': 'ira_colder_temp', 'db_dtype': 'int','factor': 1},
    6: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_second_power', 'hdf_dtype': 'int', 'columns': 3,
        'db_field': 'ira_second_power', 'db_dtype': 'int','factor': 1},
    7: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_pc_power', 'hdf_dtype': 'int', 'columns': 4,
        'db_field': 'ira_pc_power', 'db_dtype': 'int','factor': 1},              
    8: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_turn', 'hdf_dtype': 'int', 'columns': 3,
        'db_field': 'ira_turn', 'db_dtype': 'int','factor': 1},   
    9: {'src': 'l1b_obc', 'dims': 1, 'fill_value': 0,
        'sds': 'Telemetry/ira_temp_control', 'hdf_dtype': 'int', 'columns': 1,
        'db_field': 'ira_temp_control', 'db_dtype': 'int','factor': 1},            
}

sds_name = {
    1: {'name': 'ira_black_temp1',},
    2: {'name': 'ira_black_temp2',},
    3: {'name': 'ira_black_temp3',},
    4: {'name': 'ira_black_temp4',},
    5: {'name': 'ira_modulator_temp1',},
    6: {'name': 'ira_modulator_temp2',},
    7: {'name': 'ira_modulator_temp3',},
    8: {'name': 'ira_modulator_temp4',},
    9: {'name': 'ira_wheel_temp1',},
    10: {'name': 'ira_wheel_temp2',},
    11: {'name': 'ira_wheel_temp3',},
    12: {'name': 'ira_wheel_temp4',},
    13: {'name': 'ira_parts_temp1',},
    14: {'name': 'ira_parts_temp2',},
    15: {'name': 'ira_parts_temp3',},
    16: {'name': 'ira_parts_temp4',},
    17: {'name': 'ira_parts_temp5',},
    18: {'name': 'ira_colder_temp1',},
    19: {'name': 'ira_colder_temp2',},
    20: {'name': 'ira_second_power1',},
    21: {'name': 'ira_second_power2',},
    22: {'name': 'ira_second_power3',},
    23: {'name': 'ira_pc_power1',},
    24: {'name': 'ira_pc_power2',},
    25: {'name': 'ira_pc_power3',},
    26: {'name': 'ira_pc_power4',},
    27: {'name': 'ira_turn1',},
    28: {'name': 'ira_turn2',},
    29: {'name': 'ira_turn3',},
    30: {'name': 'ira_temp_control',},
            
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
                    11: '5000', 12: '5000', 13: '5000',14: '5000', 15: '5000', 
                    16: '5000', 17: '5000', 18: '5000', 19: '5000', 20: '5000'} },
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
                    11: '5000', 12: '5000', 13: '5000',14: '5000', 15: '5000', 
                    16: '5000', 17: '5000', 18: '5000', 19: '5000', 20: '5000'} },
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
                    11: '5000', 12: '5000', 13: '5000',14: '5000', 15: '5000', 
                    16: '5000', 17: '5000', 18: '5000', 19: '5000', 20: '5000'} },
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
                    11: '5000', 12: '5000', 13: '5000',14: '5000', 15: '5000', 
                    16: '5000', 17: '5000', 18: '5000', 19: '5000', 20: '5000'} },
            3: {'type': 'min_max', 'fmt': 'lat', 'fields_dims': 2},
        },
    },            
    'group_by': '',
}


sql_rttov_by_point = {
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
                    1: '5000', 2: '5000', 3: '5000', 4: '5000', 5: '5000', 
                    6: '5000', 7: '5000', 8: '5000', 9: '5000', 10: '5000', 
                    11: '5000', 12: '5000', 13: '5000',14: '5000', 15: '5000', 
                    16: '5000', 17: '5000', 18: '5000', 19: '5000', 20: '5000'} },
            #3: {'type': 'min_max', 'fmt': 'lat', 'fields_dims': 2},
            3: {'type': 'point', 'fmt': 'scpt', 'fields_dims': 1},
        },
    },            
    'group_by': '',
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
                    11: '2000', 12: '2000', 13: '2000',14: '2000',15: '2000',
                    16: '2000',17: '2000',18: '2000',19: '2000',20: '2000'} },
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
                    11: '2000', 12: '2000', 13: '2000',14: '2000',15: '2000',
                    16: '2000',17: '2000',18: '2000',19: '2000',20: '2000'} },
            #3: {'type': 'min_max', 'fmt': 'lat', 'fields_dims': 2},
            3: {'type': 'point', 'fmt': 'scpt', 'fields_dims': 1},
            4: {'type': 'realtime_start', 'param_fmt': 'ymdhms','fields_dims': 1},
            5: {'type': 'realtime_end', 'param_fmt': 'ymdhms','fields_dims': 1},
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












