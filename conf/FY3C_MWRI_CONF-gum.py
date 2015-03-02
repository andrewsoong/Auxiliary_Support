#/usr/bin/env python
# -*- coding: utf-8 -*-

"""FY3C MWRI configuration."""

__author__ = 'gumeng'

#channels for your instrument
channels = 10

#pixels for your instrument
# for MWRI, the pixels are NOT uniq, we should set it in future as possible.
pixels = 254

#scans attribute name in HDF
scans_name = 'Number Of Scans'

# src: l1a, l1b, obc, geo.
# dims: 3 means that is channels*scans*pixel, 
#    so, we should create table like: obs_bt1, obs_bt2, obs_bt3, obs_bt4
# hdf_dtype: datatype in HDF.
# db_dtype: datatype in db. we just save %.4f when float: 1.23456 --> 1.2345
# factor: be used ONLY if hdf_datatype != db_datatype, such as:
#    int = int( float * 100 )
# idx MUST start from 1.

# latitude sds.
lat_sds_to_db = {
    1: {'src': 'l1b', 
        'sds': 'Geolocation/Latitude', 'hdf_dtype': 'float', 'dims': 2, 
        'db_field': 'lat', 'db_dtype': 'int', 'factor': 100}                 
}

# common sds.
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
        'sds': 'Data/LandSeaMask', 'hdf_dtype': 'tinyint unsigned', 'dims': 2, 
        'db_field': 'landsea', 'db_dtype': 'tinyint unsigned', 'factor': 1},
    7: {'src': 'l1b', 
        'sds': 'Data/DEM', 'hdf_dtype': 'smallint', 'dims': 2, 
        'db_field': 'dem', 'db_dtype': 'smallint', 'factor': 1},
    8: {'src': 'l1b', 'sds': 'Data/EARTH_OBSERVE_BT_10_to_89GHz', 
        'hdf_dtype': 'smallint', 'dims': 3, 
        'db_field': 'obs_bt', 'db_dtype': 'smallint', 'factor': 1},
}

# simulation field in db. one for rttov, another for crtm.
# How we get sim filed in db?
# 1. set sim_method as sim filed's prefix: rttov, crtm
# 2. combine sim_method and rttov[|crtm]_sim_to_db like:
#    rttov_sim_bt1, rttov_sim_bt2, ...
#    crtm_sim_bt1, crtm_sim_bt2, ...
rttov_sim_to_db = {
    1: {'db_field': 'rttov_bt', 'dims': 3, 'db_dtype': 'smallint'},
    2: {'db_field': 'rttov_nwp_begin_t', 'dims': 1, 'db_dtype': 'float'},
    3: {'db_field': 'rttov_nwp_begin_coef', 'dims': 1, 'db_dtype': 'float'},
    4: {'db_field': 'rttov_nwp_end_t', 'dims': 1, 'db_dtype': 'float'},
    5: {'db_field': 'rttov_nwp_end_coef', 'dims': 1, 'db_dtype': 'float'},
}
crtm_sim_to_db = {
    1: {'db_field': 'crtm_bt', 'dims': 3, 'db_dtype': 'smallint'},
    2: {'db_field': 'crtm_nwp_begin_t', 'dims': 1, 'db_dtype': 'float'},
    3: {'db_field': 'crtm_nwp_begin_coef', 'dims': 1, 'db_dtype': 'float'},
    4: {'db_field': 'crtm_nwp_end_t', 'dims': 1, 'db_dtype': 'float'},
    5: {'db_field': 'crtm_nwp_end_coef', 'dims': 1, 'db_dtype': 'float'},
}

time_sds_to_db = {
    'daycnt': {'src': 'obc', 'sds': 'Calibration/Scan_daycnt', 
               'hdf_dtype': 'int', 'dims': 2},
    'mscnt': {'src': 'obc', 'sds': 'Calibration/Scan_mscnt', 
              'hdf_dtype': 'float', 'dims': 2},
}

# simulation result bin fmt
# one record = 35 int + 2 nwp file time + 2 nwp coef float
# rttov_sim_fmt = '=' + 'i'*35 + 'd'*2 + 'f'*2
# crtm_sim_fmt = '=' + 'i'*35 + 'd'*2 + 'f'*2
sim_fmt = '=' + 'i'*35 + 'd'*2 + 'f'*2

# if default value found in sim's result bin data, we should 
# trans to db default value instead.
sim_default_val = -2147483648
db_default_val = -32768

# sim bin data we can use, idx start from 0.
# idx order MUST follow strictly of sim_to_db field sequence.
# dims: 3 means that is channels*scans*pixel, we should create table like: 
#    sim_bt1, sim_bt2, ..., sim_bt[channels]
#    dims value = 3 or 1, and, if dims=3, the data idx should increase 
#    continuely to (idx + channels) 
sim_data_idx = {
    1: {'idx': 23, 'field': 'sim_bt', 'dims': 3},
    2: {'idx': 35, 'field': 'nwp_begin_t', 'dims': 1},
    3: {'idx': 37, 'field': 'nwp_begin_coef', 'dims': 1},
    4: {'idx': 36, 'field': 'nwp_end_t', 'dims': 1},
    5: {'idx': 38, 'field': 'nwp_end_coef', 'dims': 1},
}

# time data is int fmt. ONLY used in sim bin data.
sim_time_idx = {
    'idx_year': 7, 'idx_mon': 8, 'idx_day': 9, 'idx_hour':10, 
    'idx_min': 11, 'idx_sec': False,
}

# functions in sql fmt. this is just for ONE chanel !!! sql like:
# select  sim_mod, count(*), avg(obs_bt3/100 - sim_bt3/100), 
#     STDDEV_POP(obs_bt3/100 - sim_bt3/100) from ( 
#        select sim_mod, obs_bt3, sim_bt3 from 
#            FY3B_MWTSX_GBAL_L1_20131110_0220_060KM_MS union all 
#        select sim_mod, obs_bt3, sim_bt3 from 
#            FY3B_MWTSX_GBAL_L1_20131110_1048_060KM_MS ) 
# as total group by sim_mod;
# here we split this sql to the following slice, in which,
# 'param_dim_cnt': 2 means %s %s used in 'param_fmt': 'obs_bt%s - sim_bt%s'
sql_calc_stat_hourly = {
    'prefix': 'select sim_mod, count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 'param_fmt': 'obs_bt%s/100 - sim_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 'param_fmt': 'obs_bt%s/100 - sim_bt%s/100', 'param_fmt_cnt': 2, },
    },
    'sub_sql': {
        'prefix': 'select sim_mod',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'sim_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'sim_bt', 'fields_dims': 3, 'not_equal': '-2147483648'},
        },
    },            
    'group_by': 'group by sim_mod',
}

sql_calc_stat_lat = {
    'prefix': 'select sim_mod, count(*)',
    'func': {
        1: {'name': 'avg', 'param_dims': 3, 'param_fmt': 'obs_bt%s/100 - sim_bt%s/100', 'param_fmt_cnt': 2},
        2: {'name': 'STDDEV_POP', 'param_dims': 3, 'param_fmt': 'obs_bt%s/100 - sim_bt%s/100', 'param_fmt_cnt': 2, },
    },
    'sub_sql': {
        'prefix': 'select sim_mod',
        'fields': {
            1: {'fmt': 'obs_bt', 'fields_dims': 3},
            2: {'fmt': 'sim_bt', 'fields_dims': 3},
        },
        'where': {
            1: {'type': 'not_equal', 'fmt': 'sim_bt', 'fields_dims': 3, 'not_equal': '-2147483648'},
            2: {'type': 'min_max', 'fmt': 'lat', 'fields_dims': 2},
            #3: {'type': 'min_max', 'fmt': 'lon', 'fields_dims': 2},
        },
    },            
    'group_by': 'group by sim_mod',
}














