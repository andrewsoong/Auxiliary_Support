#/usr/bin/env python

"create by gumeng."

#channels for your instrument
channels = 20 # 20 = 26 - 6, the last 6 channels are NOT used at all!

#pixels for your instrument
# for MWRI, the pixels are NOT uniq, so we should get pixels num from HDF.attri.
pixels = 56

#scans attribute name in HDF
scans_name = 'Number Of Scans'

#db infos.
data_db = 'FY3C_IRAS'

stat_db = 'LIFE_CYCLE_STAT'
stat_table = {
    '6': 'FY3C_IRAS_6H', 
    '12': 'FY3C_IRAS_12H',
    'lat': 'FY3C_IRAS_12H_LAT',
}

#SDS.name, datatype, dims, field.name.in.table, type, factor for float->int
#where,
# dims: 3 means that is channels*scans*pixel, we should create table like: obs_bt1, obs_bt2, obs_bt3, obs_bt4
# datatype: datatype in HDF.
# type: datatype in db.
# factor: be used ONLY if datatype!=type, like int = int( float * 100 ), we should do ceil()!
#    More ofen, we should always set datatype=type, and sim's datatype=HDF.datatype.
sds_to_db = {
    1: {'sds': 'Geolocation/Latitude', 'datatype': 'float', 'dims': 2, 'field': 'lat', 'type': 'float', 'factor': 1},
    2: {'sds': 'Geolocation/Longitude', 'datatype': 'float', 'dims': 2, 'field': 'lon', 'type': 'float', 'factor': 1},
    3: {'sds': 'Geolocation/SensorZenith', 'datatype': 'int', 'dims': 2, 'field': 'sen_zen', 'type': 'int', 'factor': 1},
    4: {'sds': 'Geolocation/SensorAzimuth', 'datatype': 'int', 'dims': 2, 'field': 'sen_az', 'type': 'int', 'factor': 1},
    5: {'sds': 'Geolocation/SolarZenith', 'datatype': 'int', 'dims': 2, 'field': 'solar_zen', 'type': 'int', 'factor': 1},
    6: {'sds': 'Geolocation/SolarAzimuth', 'datatype': 'int', 'dims': 2, 'field': 'solar_az', 'type': 'int', 'factor': 1},
    7: {'sds': 'Geolocation/LandSeaMask', 'datatype': 'int', 'dims': 2, 'field': 'landsea', 'type': 'int', 'factor': 1},
    8: {'sds': 'Geolocation/DEM', 'datatype': 'int', 'dims': 2, 'field': 'dem', 'type': 'int', 'factor': 1},
    9: {'sds': 'Data/IRAS_TB', 'datatype': 'float', 'dims': 3, 'field': 'obs_bt', 'type': 'int', 'factor': 100},
}

# be used ONLY if time_fmt='daycnt' or 'mscnt'
time_sds_to_db = {
    1: {'sds': 'daycnt', 'datatype': 'int', 'dims': 2, 'is_day': True},
    2: {'sds': 'mscnt', 'datatype': 'int', 'dims': 2, 'is_day': False},
}

# simulation field in db
sim_to_db = {
    1: {'field': 'sim_bt', 'dims': 3, 'type': 'int'},
    2: {'field': 'nwp_begin_t', 'dims': 1, 'type': 'float'},
    3: {'field': 'nwp_begin_coef', 'dims': 1, 'type': 'float'},
    4: {'field': 'nwp_end_t', 'dims': 1, 'type': 'float'},
    5: {'field': 'nwp_end_coef', 'dims': 1, 'type': 'float'},
}

#simulation result bin fmt
# one record = 51 int + 2 nwp file time + 2 nwp coef float
sim_fmt = '=' + 'i'*51 + 'd'*2 + 'f'*2

#sim bin data we can use, idx start from 0.
# idx order MUST follow strictly of sim_to_db field sequence.
# dims: 3 means that is channels*scans*pixel, we should create table like: sim_bt1, sim_bt2, ..., sim_bt[channels]
# dims value = 3 or 1, and, if dims=3, the data idx should increase continuely to (idx + channels) 
sim_data_idx = {
    1: {'idx': 23, 'field': 'sim_bt', 'dims': 3},
    2: {'idx': 51, 'field': 'nwp_begin_t', 'dims': 1},
    3: {'idx': 52, 'field': 'nwp_end_t', 'dims': 1},
    4: {'idx': 53, 'field': 'nwp_begin_coef', 'dims': 1},
    5: {'idx': 54, 'field': 'nwp_end_coef', 'dims': 1},
}
# time data is int fmt. ONLY used in sim bin data.
sim_time_idx = {
    'idx_year': 7, 'idx_mon': 8, 'idx_day': 9, 'idx_hour':10, 'idx_min':11, 'idx_sec': False,
}

need_calc_stat_hourly = True
#functions in sql fmt. this is just for ONE chanel !!! sql like:
# select  sim_mod, count(*),avg(obs_bt3/100 - sim_bt3/100), STDDEV_POP(obs_bt3/100 - sim_bt3/100) from 
# ( select sim_mod, obs_bt3, sim_bt3 from FY3B_MWTSX_GBAL_L1_20131110_0220_060KM_MS union all 
# select sim_mod, obs_bt3, sim_bt3 from FY3B_MWTSX_GBAL_L1_20131110_1048_060KM_MS ) as total group by sim_mod;
# here we split this sql to the following slice, in which,
# 'param_dim_cnt': 2 means 2 %s used in 'param_fmt': 'obs_bt%s - sim_bt%s'
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














