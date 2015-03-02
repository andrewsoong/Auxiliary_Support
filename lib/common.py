#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Public function, provide for others call."""

__author__ = 'wzq'

# Copyright (c) 2014, shinetek.
# All rights reserved.    
#    
# date			author		changes
# 2014-07-25	gumeng		
# 2014-07-24	gumeng		10.24.182.83 Sugon luster IOError, add try-except
# 2014-06-16	gumeng		change tuple_for_db() for fy3c.mwri.intercept,slope
# 2014-03-27	gumeng		del phplog(), add info(), debug(), ...
# 2014-02-10	gumeng		format to python style.
# 2013-12-09	wangzq		create.

import time
import signal
import sys
import os
import re
import numpy
import struct
import types
import socket
import shutil
import commands
import string
import fcntl
import h5py as h5
from dateutil import tz
from datetime import datetime
from datetime import timedelta

sys.path.append('/home/fymonitor/MONITORFY3C/py2/conf')
import conf

#return host name
def get_local_hostname():
	return socket.gethostname()

# return utc time string, like: 2014-01-02 03:40:06
def utc_nowtime():
	from_zone=tz.gettz('UTC')
	utc=datetime.utcnow()
	utc=utc.replace(tzinfo=from_zone)
	local=utc.astimezone(from_zone)
	cur_time=datetime.strftime(local,'%Y-%m-%d %H:%M:%S')
	return cur_time

# return utc date like : 20131220
def utc_nowdate():
	from_zone=tz.gettz('UTC')
	utc=datetime.utcnow()
	utc=utc.replace(tzinfo=from_zone)
	local=utc.astimezone(from_zone)
	cur_time=datetime.strftime(local,'%Y%m%d')
	return cur_time

# return utc date like : 20140102034537
def utc_YmdHMS():
	from_zone=tz.gettz('UTC')
	utc=datetime.utcnow()
	utc=utc.replace(tzinfo=from_zone)
	local=utc.astimezone(from_zone)
	cur_time=datetime.strftime(local,'%Y%m%d%H%M%S')
	return cur_time

# return utc date like : 2014-04-24-18
def utc_YmdH():
	from_zone=tz.gettz('UTC')
	utc=datetime.utcnow()
	utc=utc.replace(tzinfo=from_zone)
	local=utc.astimezone(from_zone)
	cur_time=datetime.strftime(local,'%Y-%m-%d-%H')
	return cur_time

# just for test in 10.24.2.158, change timespan to last month.
def get_last_mon_calc_timespan(timespan):
	ret = {'begin_str': '', 'begin_t':0, 'end_str': '', 'end_t':0}
	
	# '2014-04-01 23:12:34'
	begin_mon = int(timespan['begin_str'][5:7]) - 1
	ret['begin_str'] = timespan['begin_str'][0:5] + format(begin_mon,'02d') \
					+ timespan['begin_str'][7:]
	end_mon = int(timespan['end_str'][5:7]) - 1
	ret['end_str'] = timespan['end_str'][0:5] + format(end_mon,'02d') \
					+ timespan['end_str'][7:]

	ret['begin_t'] = time.mktime(time.strptime(ret['begin_str'],
											"%Y-%m-%d %H:%M:%S") )
	ret['end_t'] = time.mktime(time.strptime(ret['end_str'],
												"%Y-%m-%d %H:%M:%S") )
	
	return ret

# if hour_span = 6 and calc_date's hour locate at range(0,6) [00:00 -> 05:59], 
# we should calc for yesterday from 18:00 to today 00:00.
# src_date: calc data like '2014-04-09-01' as Y-m-d-H
# hour_span: 6 or 12 is support now
# return:
#	'begin_str': time str of begin time
#	'end_str': time str of end time
#	'begin_t': time stamp of begin time
#	'end_t': time stamp of end time
def get_calc_timespan(src_date, hour_span):
	ret = {'begin_str': '', 'begin_t':0, 'end_str': '', 'end_t':0}
	
	calc_for_timespan = {
	    '6': {
			# span: time range. yday: yesterday.
	        0: {'span': range(0, 6), 'yday': True, 'begin': '18:00:00',
				 'end': '00:00:00'},
	        1: {'span': range(6,12), 'yday': False, 'begin': '00:00:00',
			 	'end': '06:00:00'},
	        2: {'span': range(12,18), 'yday': False, 'begin': '06:00:00',
			 	'end': '12:00:00'},
	        3: {'span': range(18,24), 'yday': False, 'begin': '12:00:00',
			 	'end': '18:00:00'},
	    },
	    '12': { 
	        0: {'span': range(0,12), 'yday': True, 'begin': '12:00:00',
			 	'end': '00:00:00'},
	        1: {'span': range(12,24), 'yday': False, 'begin': '00:00:00',
			 	'end': '12:00:00'},
	    },
	    '24': { 
	        0: {'span': range(0,23), 'yday': True, 'begin': '00:00:00',
			 	'end': '00:00:00'},
	    },
	}
	
	timeArray = time.strptime(src_date[0:len('2014-05-06')], "%Y-%m-%d")
	src_timestamp = time.mktime(timeArray)
	
	src_hour = time.strptime(src_date, "%Y-%m-%d-%H").tm_hour
	for one_span in calc_for_timespan[hour_span].values():
		if src_hour not in one_span['span']:
			continue
		
		calc_for_timestamp = src_timestamp
		# if yesterday, the begin time is yesterday.
		if one_span['yday']:
			calc_for_timestamp = src_timestamp - 24*60*60
			
		ret['begin_str'] = time.strftime("%Y-%m-%d", 
										time.localtime(calc_for_timestamp)) \
						 + ' ' + one_span['begin']
		ret['begin_t'] = time.mktime(time.strptime(ret['begin_str'],
													"%Y-%m-%d %H:%M:%S") )

		# if yesterday, the end time is today.
		if one_span['yday']:
			calc_for_timestamp = src_timestamp
			
		ret['end_str'] = time.strftime("%Y-%m-%d",
										time.localtime(calc_for_timestamp)) \
						 + ' ' + one_span['end']
		ret['end_t'] = time.mktime(time.strptime(ret['end_str'],
													"%Y-%m-%d %H:%M:%S") )
		# here, we can return
		return ret

# check data validation, if data item is None, set default value.
# if data item is NOT None, return data/factor
def get_data_with_default(data, factor = 1, default = 0):
	ret = []
	for one_data in data:
		if one_data is None:
			ret.append(default)
		else:
			ret.append(float(one_data)/factor )
			
	return ret

# select count(cold_ang1), min(cold_ang1), max(cold_ang1), STDDEV_POP(cold_ang1) 
# from (
#    select cold_ang1 from FY3C_MWTSX_GBAL_L1_20140731_0119_OBCXX_MS where 
#    cold_ang1!=-99000 union all 
#    select cold_ang1 from FY3C_MWTSX_GBAL_L1_20140731_0300_OBCXX_MS where 
#    cold_ang1!=-99000
# ) as total;
def get_calc_daily_sql(field, tables, fill_value, 
						prefix_sql, subsql, postfix_sql):
	
	prefix = prefix_sql % tuple( [field]*5 )
	subsql_total = ' union all '.join( map( lambda x, y: 
											subsql%(field, x, field, y),
											tables, [fill_value]*len(tables)))
	return prefix + subsql_total + postfix_sql

# select count(cold_ang1), min(cold_ang1), max(cold_ang1), STDDEV_POP(cold_ang1) 
# from (
#    select cold_ang1 from FY3C_MWTSX_GBAL_L1_20140731_0119_OBCXX_MS where 
#    channel=1 and cold_ang1!=-99000 union all 
#    select cold_ang1 from FY3C_MWTSX_GBAL_L1_20140731_0300_OBCXX_MS where 
#    channel=1 and cold_ang1!=-99000
# ) as total;
def get_calc_daily_channel_sql(channel, field, tables, fill_value, 
								prefix_sql, subsql, postfix_sql):
	
	prefix = prefix_sql % tuple( [field]*5 )
	subsql_total = ' union all '.join( map( lambda x, y, z: 
											subsql%(field, x, y, field, z),
											tables, [channel]*len(tables),
											[fill_value]*len(tables) ) )
	return prefix + subsql_total + postfix_sql

# select ymdhms, ins_temp, ... from (
#	select ymdhms, ins_temp, ... from FY3C_MWTSX_GBAL_L1_20140429_0014_OBCXX_MS
#	union all
#	select ymdhms, ins_temp, ... from FY3C_MWTSX_GBAL_L1_20140429_0155_OBCXX_MS
# ) as total
def get_obc_2dim_sql(obc_setting, channels, obc_table, select_prefix):
	fields = get_sql_dict(channels, obc_setting, showtype = False)['field']
	sub_select = select_prefix + fields + ' from '
	return sub_select + ' ( ' \
			+ ' union all '.join(map(lambda x: sub_select + x,  obc_table) ) \
			+ ' ) as total'

# select ymdhms, cal_coef1, ... from (
#	select ymdhms, cal_coef1, ... from FY3C_MWTSX_GBAL_L1_20140429_0014_OBCXX_MS
#	where channel = x
#	union all
#	select ymdhms, cal_coef1, ... from FY3C_MWTSX_GBAL_L1_20140429_0155_OBCXX_MS
#	where channel = x
# ) as total
def get_obc_3dim_sql(obc_setting, channel, obc_table, select_prefix, where):
	fields = get_sql_dict(1, obc_setting, showtype = False)['field']
	sub_select = select_prefix + fields + ' from '
	return sub_select + ' ( ' \
			+ ' union all '.join(map(lambda table: \
							sub_select + table + where + channel, obc_table)) \
			+ ' ) as total'

# change '2014-03-10 06:20:18' to ['2014', '03', '10', '06', '20', '18']
def time_to_arr(data):
    return [data[0:4], data[5:7], data[8:10], \
		  	data[11:13], data[14:16], data[17:19]]

# get sql of create table for data db.
# input: sds_setting, sim_setting
# output: sql frag, like "obs_bt1 int, obs_bt2 int, sim_bt1 int, sim_bt2 int"
def get_create_table_sql(channels, total_fields):
	showtype = True
	return get_sql_dict(channels, total_fields, showtype)['field']

# if dims=3, the record in fy3b.mwts, 4 channels, db.table shoule like:
# "obs_bt1 int, obs_bt2 int, obs_bt3 int, obs_bt4 int"
def get_inner_sql(channels, field, type, showtype = True):
	return ', '.join(get_inner_sql_tuple(channels, field, type, showtype))

# return tuple = (obs_bt1 int, obs_bt2 int, obs_bt3 int, obs_bt4 int)
def get_inner_sql_tuple(channels, field, type, showtype = True):
	if not channels or channels==1:
		if showtype:
			yield field + ' ' + type
		else:
			yield field
	else:
		for i in range(1, channels + 1):
			if showtype:
				yield field + str(i) + ' ' + type
			else:
				yield field + str(i)
		
# like sql:
# 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
# we just return sql dict string, that is:
# sql['field'] = 'obs_bt1, obs_bt2, obs_bt3, ...'
# 	and, if showtype=True, sql['field'] = 'obs_bt1 int, obs_bt2 int, ...'
# sql['value'] = '%s, %s, %s, %s'
def get_sql_dict(channels, sds_setting, showtype = True):
	list = get_sql_dict_list(channels, sds_setting, showtype)
	return {'field': ', '.join(list['field']), 
			'value': ', '.join(list['value'])}

# like sql:
# 'insert into %s(id, obs_bt1, obs_bt2, obs_bt3) values(%s, %s, %s, %s)'
# we just return sql dict list, that is:
# sql['field'] = ['obs_bt1', 'obs_bt2', ...]
# 	and, if showtype=True, sql['field'] = ['obs_bt1 int', 'obs_bt2 int', ...]
# sql['value'] = ['%s', '%s', ...]
def get_sql_dict_list(channels, sds_setting, showtype = True):
	field = []
	value = []
	for one_sds in sds_setting:
		if one_sds['dims'] == 2 or one_sds['dims'] == 1:
			if 'columns' in one_sds:
				value.extend(['%s']*one_sds['columns'] )
				field.extend(get_inner_sql_tuple(one_sds['columns'], 
												one_sds['db_field'], 
											 	one_sds['db_dtype'], showtype))
			else:
				value.extend(['%s'])
				field.extend(get_inner_sql_tuple(None, 
												one_sds['db_field'], 
											 	one_sds['db_dtype'], showtype))
		elif one_sds['dims'] == 3:
			if 'columns' in one_sds:
				value.extend( ['%s']*one_sds['columns'] )
				field.extend(get_inner_sql_tuple(one_sds['columns'],
												 one_sds['db_field'], 
												 one_sds['db_dtype'], showtype) )
			else:
				value.extend( ['%s']*channels )
				field.extend(get_inner_sql_tuple(channels, one_sds['db_field'], 
												 one_sds['db_dtype'], showtype) )
		else:
			pass			

	return {'field': field, 'value': value}

def near_int(input):
	try:
		if input >= 0:
			return int(input + 0.5)
		else:
			return -(int(abs(input - 0.5)))
	except:
		return 65535

# get proper data fmt for db from sds
# eg:
# sds data should *100 if src_datatype!=dest_datatype
def tuple_for_db(src_data, sds_setting):
	# if hdf dtype is same with db dtype, we can return data directly.
	if 0 == cmp(sds_setting['hdf_dtype'], sds_setting['db_dtype']):
		if 'intercept' in sds_setting.keys() \
			and 'slope' in sds_setting.keys():
			return map(lambda x: 
						sds_setting['intercept'] + x*sds_setting['slope'],
						src_data)
		else:
			return src_data
		
	#if hdf dtype is NOT same with db dtype, we should translate data.
	if 'intercept' in sds_setting.keys() \
		and 'slope' in sds_setting.keys():
		dest_data = map(lambda x: 
						sds_setting['intercept'] + x*sds_setting['slope'],
						src_data)
	else:
		dest_data = src_data
		
	return map(near_int, [x*sds_setting['factor'] for x in dest_data])

# if src_data = sec_default, set to dest_default
def change_default(src_data, src_default, dest_default):
	for data in src_data:
		if data == src_default:
			yield dest_default
		else:
			yield data
	
# get proper data fmt for db from sds
# src_data fmt is whatever like channel*scans*pixel or scans*pixel.
# dest_data 3 dim is ONLY channel*scans*pixel fmt.
def trans_data_for_db(src_data, sds_setting):
	dest_data = []

	if(sds_setting['dims'] == 3):
		if(sds_setting['rank']['x'] == 'channel'
			and sds_setting['rank']['y'] == 'scan'
			and sds_setting['rank']['z'] == 'pixel'):
			for idx in xrange(0,sds_setting['use_ch']):
				channel_data = []
				for one_scan in src_data[idx, : , :]:
					channel_data.append(tuple_for_db(one_scan, sds_setting))
				dest_data.append(channel_data)
		elif(sds_setting['rank']['x'] == 'scan'
			and sds_setting['rank']['y'] == 'pixel'
			and sds_setting['rank']['z'] == 'channel'):
			for idx in xrange(0,sds_setting['use_ch']):
				channel_data = []
				for one_scan in src_data[: , : , idx]:
					channel_data.append(tuple_for_db(one_scan, sds_setting))
				dest_data.append(channel_data)
		elif(sds_setting['rank']['x'] == 'scan'
			and sds_setting['rank']['y'] == 'channel'
			and sds_setting['rank']['z'] == 'pixel'):
			for idx in xrange(0,sds_setting['use_ch']):
				channel_data = []
				for one_scan in src_data[: , idx, :]:
					channel_data.append(tuple_for_db(one_scan, sds_setting))
				dest_data.append(channel_data)
	elif(sds_setting['dims'] == 2):
		for one_scan in src_data:
			dest_data.append(tuple_for_db(one_scan, sds_setting))
	elif(sds_setting['dims'] == 1):
		for one_scan in src_data:
			dest_data.append(tuple_for_db([one_scan], sds_setting))

	return dest_data

# get proper data fmt for db from numpy.narray, not hdf.sds
# src_data fmt MUST be [1,2,3...] list.
def trans_list_data_for_db(src_data, sds_setting):
	dest_data = []

	if(sds_setting['dims'] == 1):
		dest_data = tuple_for_db(src_data, sds_setting)

	return dest_data

# avg(src_data) near_int
def get_avg(data, sds_setting):
	total = 0.0
	for idx in sds_setting['avg_idx']:
		total += data[idx]
		
	return near_int(total/len(sds_setting['avg_idx']))
	
# avg(src_data) for db field by sds_setting formula.
def avg_data_for_db(src_data, sds_setting):
	dest_data = []

	sds_len = len(sds_setting)
	for i in xrange(1, sds_len + 1):
		if sds_setting[i]['dims'] == 3 and sds_setting[i]['need_avg']:
			one_sds = []
			for one_channel in src_data[i - 1]:
				channel_data = []
				for one_scan in one_channel:
					channel_data.append([get_avg(one_scan, sds_setting[i])] )
				one_sds.append(channel_data)
			dest_data.append(one_sds)
		else:
			dest_data.append(src_data[i - 1])

	return dest_data

# return record-list of unpacked fmt
def get_record_from_sim(filename, fmt):
    record = []

    size = struct.calcsize(fmt)
    
    try:
        file_object = open(filename,'rb')
        while True:
	        one_record = file_object.read(size)
	        if not one_record:
	            break
	        record.extend([struct.unpack(fmt, one_record)])
    except (OSError, IOError) as e:
		msg = 'get_record_from_sim error[' + str(e.args[0])+']: ' + e.args[1] 
		print msg
    except (struct.error) as e:
		msg = 'get_record_from_sim error: ', e
		print msg
    finally:
		file_object.close()

    return record

# sql like:
# select count(*), avg(obs_bt3/100 - crtm_bt3/100), STDDEV_POP(obs_bt3/100 - 
# 		crtm_bt3/100) from ( 
#	select obs_bt3, crtm_bt3 from FY3B_MWTSX_GBAL_L1_20131110_0220_060KM_MS 
#	union all 
#	select obs_bt3, crtm_bt3 from FY3B_MWTSX_GBAL_L1_20131110_1048_060KM_MS 
# ) as total
def get_sql_calc_stat(channel, sql_fmt, table_name):
	select = sql_fmt['prefix']
	
	func_list = map(get_sql_func_str, 
					[channel]*len(sql_fmt['func']), 
					sql_fmt['func'].values() ) 
	
	sub_sql_select = get_sql_sub_select(channel, sql_fmt['sub_sql'])
	
	sub_sql_where = get_sql_sub_where(channel, sql_fmt['sub_sql'])
	
	one_sub_sql = map(lambda x,y,z: x + ' from ' + y + ' ' + z,
						[sub_sql_select]*len(table_name), 
						table_name, 
						[sub_sql_where]*len(table_name) )
	
	sql = select + ', ' + ', '.join(func_list) + ' from ( ' \
		+ ' union all '.join(one_sub_sql) + ' ) as total_table ' \
		+ sql_fmt['group_by'] 
	return sql

def get_sql_calc_stat_realtime(channel, sql_fmt, table_name):
	select = sql_fmt['prefix']
	
	func_list = map(get_sql_func_str_realtime, 
					[channel]*len(sql_fmt['func']), 
					sql_fmt['func'].values() ) 
	
	sub_sql_select = get_sql_sub_select(channel, sql_fmt['sub_sql'])
	
	sub_sql_where = get_sql_sub_where_realtime(channel, sql_fmt['sub_sql'])

	one_sub_sql = map(lambda x,y,z: x + ' from ' + y + ' ' + z,
						[sub_sql_select]*len(table_name), 
						table_name, 
						[sub_sql_where]*len(table_name) )
	
	sql = select + ', ' + ', '.join(func_list) + ' from ( ' \
		+ ' union all '.join(one_sub_sql) + ' ) as total_table ' \
		+ sql_fmt['group_by'] 
	return sql

def get_sql_func_str_realtime(channel, func_setting):
	if func_setting['param_dims'] == 3:
		cnt = func_setting['param_fmt_cnt']
		return func_setting['name'] + '(' + \
			func_setting['param_fmt']%(tuple([channel]*cnt) ) + ')'
	elif func_setting['param_dims'] == 'point':
		cnt = func_setting['param_fmt_cnt']
		return func_setting['name'] + '(' + func_setting['param_fmt']%(tuple([channel]*cnt)) + '),2))/count(*))'
	else:
		return func_setting['name'] + '(' + func_setting['param_fmt'] + ')'	
	
# create function string in sql from func_setting like:
# {'name': 'STDDEV_POP', 'param_dims': 3, 'param_dim_cnt': 2, 'param_fmt': 'obs_bt%s - sim_bt%s'},
def get_sql_func_str(channel, func_setting):
# 	if func_setting['param_dims'] != 3:
# 		return func_setting['name'] + '(' + func_setting['param_fmt'] + ')'
# 	else:
# 		cnt = func_setting['param_fmt_cnt']
# 		return func_setting['name'] + '(' + \
# 			func_setting['param_fmt']%(tuple([channel]*cnt) ) + ')'

	if func_setting['param_dims'] == 3:
		cnt = func_setting['param_fmt_cnt']
		return func_setting['name'] + '(' + \
			func_setting['param_fmt']%(tuple([channel]*cnt) ) + ')'
	elif func_setting['param_dims'] == 'point':
		cnt = func_setting['param_fmt_cnt']
		return func_setting['name'] + '(' + func_setting['param_fmt']%(tuple([channel]*cnt)) + '),2))/count(*))'
	else:
		return func_setting['name'] + '(' + func_setting['param_fmt'] + ')'	
# create sql string from sql_fmt like:
#'sub_sql': {
#       'prefix': 'select sim_mod',
#       'fields': {
#           1: {'fmt': 'obs_bt', 'fields_dims': 3},
#           2: {'fmt': 'sim_bt', 'fields_dims': 3},
#       },
#   },
# output: select sim_mod, obs_bt1, sim_bt1
def get_sql_sub_select(channel, select_fmt):
	sql = select_fmt['prefix']
	sql += ', '.join(map(get_field_str, 
						[channel]*len(select_fmt['fields']), 
						select_fmt['fields'].values() ) )
	return sql

# create sql string from sql_fmt like:
#'sub_sql': {
#       'prefix': 'select sim_mod',
#       'fields': {
#           1: {'fmt': 'obs_bt', 'fields_dims': 3},
#           2: {'fmt': 'sim_bt', 'fields_dims': 3},
#       },
#		'where': {
#			1: {'fmt': 'sim_bt', 'fields_dims': 3, 'not_equal': -2147483648, 'min': 16000, 'max': 30000},
#		},
#   },
# output: where sim_bt1!=-2147483648
def get_sql_sub_where(channel, select_fmt):
	sql = ' where ' 
	sql += ' and '.join(map(get_where_str, 
							[channel]*len(select_fmt['where']),
							select_fmt['where'].values() ) )
	return sql

def get_sql_sub_where_realtime(channel, select_fmt):
	sql = ' where ' 
	sql += ' and '.join(map(get_where_str_realtime, 
							[channel]*len(select_fmt['where']),
							select_fmt['where'].values()) )
	return sql

def get_where_str_realtime(channel, where_fmt):
    if where_fmt['type'] == 'not_equal':
        if where_fmt['fields_dims'] != 3:
            return where_fmt['fmt'] + '!=' + where_fmt['not_equal']
        else:
            return where_fmt['fmt'] + str(channel) + '!=' \
            		+ where_fmt['not_equal']
    elif where_fmt['type'] == 'min_max':
        if where_fmt['fields_dims'] != 3:
            return ' %s <= ' + where_fmt['fmt'] + ' and ' + \
                    where_fmt['fmt'] + ' <= %s '
        else:
            return ' %s <=' + where_fmt['fmt']  + str(channel) \
                    + ' and ' + where_fmt['fmt'] + str(channel) + ' <= %s '
    elif where_fmt['type'] == 'point':
		return  where_fmt['fmt'] + ' = %s'
    elif where_fmt['type'] == 'abs':
        if where_fmt['fields_dims'] != 3:
            return 'abs(' + where_fmt['param_fmt'] \
                            + ') < ' + where_fmt['abs_value'][channel]
        else:
            return 'abs(' + where_fmt['param_fmt']% \
           				(tuple([channel]*where_fmt['param_fmt_cnt']) ) \
                        + ') < ' + where_fmt['abs_value'][channel]
                        
    elif where_fmt['type'] == 'realtime_start':
		return where_fmt['param_fmt'] + ' > \' %s \'' 
    elif where_fmt['type'] == 'realtime_end':
		return where_fmt['param_fmt'] + ' < \' %s \''


# create field string from field_fmt like:
# {'fmt': 'obs_bt', 'fields_dims': 3},
# output: obs_bt1
def get_field_str(channel, field_fmt):
	if field_fmt['fields_dims'] != 3:
		return field_fmt['fmt']
	else:
		return field_fmt['fmt'] + str(channel)

# create where string from where_fmt like:
# {'fmt': 'sim_bt', 'fields_dims': 3, 'not_equal': -2147483648, 'min': 16000, 'max': 30000},
# output: sim_bt1!=-2147483648
# min_max = (min_value, max_value)
def get_where_str(channel, where_fmt):
    if where_fmt['type'] == 'not_equal':
        if where_fmt['fields_dims'] != 3:
            return where_fmt['fmt'] + '!=' + where_fmt['not_equal']
        else:
            return where_fmt['fmt'] + str(channel) + '!=' \
            		+ where_fmt['not_equal']
    elif where_fmt['type'] == 'min_max':
        if where_fmt['fields_dims'] != 3:
            return ' %s <= ' + where_fmt['fmt'] + ' and ' + \
                    where_fmt['fmt'] + ' <= %s '
        else:
            return ' %s <=' + where_fmt['fmt']  + str(channel) \
                    + ' and ' + where_fmt['fmt'] + str(channel) + ' <= %s '
    elif where_fmt['type'] == 'point':
		return  where_fmt['fmt'] + ' = %s'
    elif where_fmt['type'] == 'abs':
        if where_fmt['fields_dims'] != 3:
            return 'abs(' + where_fmt['param_fmt'] \
                            + ') < ' + where_fmt['abs_value'][channel]
        else:
            return 'abs(' + where_fmt['param_fmt']% \
           				(tuple([channel]*where_fmt['param_fmt_cnt']) ) \
                        + ') < ' + where_fmt['abs_value'][channel]

                    
    

def rd_file(file):
	try:
		fp = open(file, 'r')
		data = fp.read()
	except IOError, e:
		print 'could not read file:', e
		return None
	
	return data

# w: overwirte file if mode not set.
def wt_file(file, data, mode = 'w'):
	try:
		fp = open(file, mode)
		fcntl.flock(fp, fcntl.LOCK_EX)
		fp.write(data)
		fcntl.flock(fp, fcntl.LOCK_UN)
		fp.close()
	except IOError,e:
		pass
# 		print 'could not write file:',e

# This Function is write log to file
# logfile_prefix: the dest log file prefix name
# level: INFO, DEBUG, ERROR, WARNING, ...
# tag: process name 
# data: The log string you want to write
def core_log(logfile_prefix, level, tag, data):
	logfile = logfile_prefix + utc_nowdate() + '.log'
	data = '[' + utc_nowtime() + ']`' + level.upper() + '`' + tag \
		+ '`' + data + '\n'
	wt_file(logfile, data, 'a')

def info(logfile_prefix, tag, data):
	core_log(logfile_prefix, 'INFO', tag, data)

def debug(logfile_prefix, tag, data):
	core_log(logfile_prefix, 'DEBUG', tag, data)

def err(logfile_prefix, tag, data):
	core_log(logfile_prefix, 'ERROR', tag, data)

def error(logfile_prefix, tag, data):
	err(logfile_prefix, tag, data)

def warn(logfile_prefix, tag, data):
	core_log(logfile_prefix, 'WARNING', tag, data)

def warning(logfile_prefix, tag, data):
	warn(logfile_prefix, tag, data)

#This function is order file by file Modify time
# @param  files:    File array
def file_sort_query(files):
	try:
		for j in range(len(files)-1,-1,-1):
			for i in range(j):
				time1=os.stat(files[i]).st_mtime
				time2=os.stat(files[i+1]).st_mtime
				if time1 > time2:
					files[i],files[i+1] = files[i+1] ,files[i]
	except OSError,e:
		msg = 'file_sort_query error[' + str(e.args[0])+']: ' + e.args[1] 
		print msg

#This function is order file array by the time and serial number among the file
# @param  files:    File array
def file_sort_by_t639_name(files):
	for j in range(len(files)-1,-1,-1):
		for i in range(j):
			file1=os.path.basename(files[i])
			file2=os.path.basename(files[i+1])
			ymd1=file1[8:16]
			ymd2=file2[8:16]
			num1=file1[16:21]
			num2=file2[16:21]
			if ymd1 > ymd2 :
				files[i],files[i+1] = files[i+1] ,files[i]
			elif ymd1 == ymd2 :
				if num1 > num2 :
					files[i],files[i+1] = files[i+1] ,files[i]
	return
				
#This function is order hdf file array by the time and serial number among the file
# @param  files:    File array
# file    like:     FY3B_MWTSX_GBAL_L1_20131123_2156_060KM_MS.HDF
def file_sort_by_hdf_name(files):
    if len(files) <= 1:
        return
	
    for j in range(len(files)-1,-1,-1):
        for i in range(j):
            file1=os.path.basename(files[i])
            file2=os.path.basename(files[i+1])
            ymd1=file1[19:27]
            ymd2=file2[19:27]
            hm1=file1[28:32]
            hm2=file2[28:32]
            time1 = long(ymd1 + hm1)
            time2 = long(ymd2 + hm2)
            if time1 > time2:
                files[i],files[i+1] = files[i+1] ,files[i]

    return

#This function is get the filepath file
# @param  path:    The file path
# @param  patten:  The file math the regular expression
# @param  filter:  The file suffix; like filter='.OK'
# @param  time_order:  The file sort rule; The default is the file modification time ascending
def get_files(path, patten='', filter='', time_order='asc'):
	files=[]
	if path[-1]!='/':
		path=path + '/'
	if os.path.isdir(path)==False:
		return files

	dh=os.listdir(path)
	for file in dh:
		if file=='.' or file=='..' or os.path.isdir(file)==True:
			continue
		if patten != '' and re.match(patten, file) == None:
			continue
		#print file
		if filter== '':
			files.append(path + file)
		else:
			dest_file = path + file[:-len(filter)]
			if os.path.isfile(dest_file)==True:
				files.append(dest_file)	
			
	file_sort_query(files)
	if time_order =='desc':
		files.reverse()
	elif time_order == 't639_name':
		file_sort_by_t639_name(files)
	elif time_order == 'hdf':
		file_sort_by_hdf_name(files)
		
	return files
		


def get_sleep_time():
	tmp = rd_file(conf.sleep_time_conf)
	sleep_time = string.atoi(tmp)
	if sleep_time < 1:
		sleep_time = conf.default_sleep_time
	return sleep_time


def isset(v): 
	try: 
		type(eval(v)) 
	except: 
		return 0 
	else: 
		return 1

# eg: min=-90, max=90, span=1
# output: [(-90, -89), (-89, -87), ..., (88, 89), (89, 90)]
def get_data_with_span(min, max, span):
	low = range(min, max, span)
	high = map(lambda x: x + span, low)
	return zip(low, high)

# ymdhms MUST be datetime type!!!
def get_time_int(ymdhms):
# 	timeArray = time.strptime(ymdhms, time_fmt)
# 	timestamp = time.mktime(timeArray)
# 	x = time.localtime(timestamp)
	return int(time.strftime('%Y%m%d%H', ymdhms.timetuple() ) )

# trans int, float, ... to string.
def data_to_str(data):
    data_type = type(data)
    if data_type is types.IntType:
        return str(data)
    elif data_type is types.FloatType:
		return format( data, '.3f')
    elif data_type is types.StringType:
		return data
    elif data_type is types.NoneType:
		return '9999'
    else:
        return str(data)

# talbe name like hdf filename
# FY3B_MWTSX_GBAL_L1_20131106_0012_060KM_MS_T639
def get_table_name(hdf_name):
    filename = os.path.basename(hdf_name)
    (table, ext) = os.path.splitext(filename)
    return table

# get scans, pixels attribution from hdf.
def get_attr_from_hdf(hdf_name, ins_conf):
    data = {'ok': False, 'scans': 0, 'pixels': 0, 'msg': None}

    if not os.path.exists(hdf_name):
        data['msg'] = hdf_name + '`not exist when get file attr.'
        return data

    try:
        f = h5.File(hdf_name, 'r')
    except:
        data['msg'] = hdf_name + '`can NOT open file.'
        return data

    # how many scans in this HDF
    try:
        scans = f.attrs[ins_conf.scans_name][0]
    except:
    	f.close()
        data['msg'] = hdf_name + '`can NOT get attr: ' + ins_conf.scans_name
        return data
     
    if scans <=0 or scans > 100000:
        msg = hdf_name + "`attr '" + ins_conf.scans_name + "' value is " \
            + str(scans) + '. do not deal with this file.'
        data['msg'] = msg
        f.close()
        return data
    else:
        data['scans'] = scans
    
    # how  many piexls in this HDF
    pixels = ins_conf.pixels
    #if 0 == cmp('MWRI', ins.upper() ):
    #    pixels = f.attrs[ins_conf.pixels_name][0]
    if pixels <=0 or pixels > 100000:
        msg = hdf_name + "`attr '" + ins_conf.pixels_name + "' value is " \
            + str(pixels) + '. do not deal with this file'
        data['msg'] = msg
        f.close()
        return data
    else:
        data['pixels'] = pixels
        
	f.close()        
    data['ok'] = True
    return data

# get time string, time stamp from time str 'ymd'
def get_time_from_str(ymd):
	span = get_calc_timespan(src_date = ymd + '-07', hour_span = '6')
	return {'str': span['begin_str'], 'timestamp': span['begin_t']}

# create time string from one sim_data
def get_time_str(sim_data, idx_setting):
    return str(sim_data[idx_setting['idx_year']])  + \
           '-' + str(sim_data[idx_setting['idx_mon']])  + \
           '-' + str(sim_data[idx_setting['idx_day']])  + \
           ' ' + str(sim_data[idx_setting['idx_hour']])  + \
           ':' + str(sim_data[idx_setting['idx_min']])  + ':00'
    
# get time total data
def get_time_data(sim_data, idx_setting):
	return map(get_time_str, sim_data, [idx_setting]*len(sim_data))

# get time string from day cnt and ms cnt
def get_time_str_from_dayms(daycnt, mscnt, time_begin):
	ms_before_12h = [ x - 43200000 for x in mscnt]
	day = map(lambda x,y: int(y) if x<0 else int(y+1), ms_before_12h, daycnt)
	ms = map(lambda x: int(x + 86400000) if x<0 else int(x), ms_before_12h)

	begin = datetime.strptime(time_begin, "%Y-%m-%d %H:%M:%S")
	
	return map(lambda x,y:
			(begin+timedelta(days=x,milliseconds=y)).strftime("%Y-%m-%d %H:%M:%S"), 
			day, ms)

# get what we need db sim data from one sim_data
# check defalut value if necessary.
def get_sim_value(sim_data, sim_setting, channels):
    sim_value = []
    for setting in sim_setting:
        to_idx = 1
        if setting['dims'] == 3:
            to_idx = channels
        
        value = sim_data[setting['idx'] : setting['idx'] + to_idx]
        changed_val = change_default(value, setting['sim_default'],
                                     setting['db_default'])
        if setting['sim_dtype'] != setting['db_dtype']:
			factor = setting['factor']
			changed_val = map(lambda x: int(x * factor), changed_val)
			        
        sim_value.extend(changed_val)
        
    return sim_value        

def get_sim_data(sim_data, sim_setting, channels):
	return map(get_sim_value, sim_data, [sim_setting.values()]*len(sim_data),
			   [channels]*len(sim_data) )

# get file infos for instrument
def get_filename_year_mon(hdf, ins_conf):
    # filename, year, mon
    filename = os.path.basename(hdf)
    year = filename[ins_conf.pre_year : ins_conf.pre_year + len('2014')]
    mon = filename[ins_conf.pre_mon : ins_conf.pre_mon + len('03')]
    return (filename, year, mon)

# get all sim output about l1b, do NOT check file existence.
def get_sim_file(sat, ins, nwp, l1b):
	sim = {'rttov': None, 'crtm': None}
	
	ins_conf_file = sat.upper() + '_' + ins.upper() + '_CONF'
	ins_conf = __import__(ins_conf_file)
	(filename, year, mon) = get_filename_year_mon(l1b, ins_conf)
	
	# /home/fymonitor/DATA/MNTOUT/FY3C/MWTS/2013/12/
	file_prefix = conf.sim_output_path + '/' + sat.upper() + '/'+ins.upper() \
				+ '/'  + year + '/' + mon + '/' + os.path.splitext(filename)[0]
	sim['rttov'] =  file_prefix + conf.sim_file[nwp]['rttov']
	sim['crtm'] = file_prefix + conf.sim_file[nwp]['crtm']

	return sim

# get all mpi sim output about l1b, do NOT check file existence.
def get_sim_bin(sat, ins, nwp, l1b):
	sim = {'rttov': None, 'crtm': None}
	
	ins_conf_file = sat.upper() + '_' + ins.upper() + '_CONF'
	ins_conf = __import__(ins_conf_file)
	(filename, year, mon) = get_filename_year_mon(l1b, ins_conf)
	
	# /home/fymonitor/DATA/MNTOUT/FY3C/MWTS/2013/12/
	# FY3C_MWHSX_GBAL_L1_20140615_0037_015KM_MS_FWDBTS_T639_CRTM202_TOVSL1X.bin
	file_prefix = conf.sim_output_path + '/' + 'SAT/NSMC/' + sat.upper() \
				+ '/' + ins.upper() + '/'  + year + '/' + mon + '/' \
				+ os.path.splitext(filename)[0]
	sim['rttov'] = file_prefix + conf.sim_bin[nwp]['rttov']
	sim['crtm'] = file_prefix + conf.sim_bin[nwp]['crtm']

	return sim

# clean all files in src folder, not recursively!!!
def empty_folder(src):
	if not os.path.isdir(src):
		return

	try:
		for item in os.listdir(src):
			itemsrc = os.path.join(src, item)
			os.remove(itemsrc)
	except (OSError, IOError) as e:
		msg = 'empty_folder error[' + str(e.args[0])+']: ' + e.args[1] 
		print msg			
		
           	
# check_ok = True or False, check ok as default.
def check_file_exist(file, check_ok = True):
	if file is None:
		# NO need file check.
		return True
	
	if os.path.isfile(file):
		if check_ok:
			if os.path.isfile(file + '.OK'):
				return True
			else:
				return False
		else:
			return True
	else:
		return False
	
# check sim OK file created or not for instrument
def sim_ok(sat, ins, nwp, l1b):
	sim = get_sim_file(sat, ins, nwp, l1b)
	
	result = map( check_file_exist, sim.values())
	for one_exist in result:
		if not one_exist:
			return False
		
	return True

# check sim bin file status. 
# FY3C_MWHSX_GBAL_L1_20140615_0037_015KM_MS_FWDBTS_T639_CRTM202_TOVSL1X.bin
def check_sim_bin(sat, ins, nwp, l1b):
	sim = get_sim_bin(sat, ins, nwp, l1b)
	
	result = map( check_file_exist, sim.values(), [False] * len(sim))
	for one_exist in result:
		if not one_exist:
			return False
		
	return True
	
def mv_hdf(hdf, dest):
	return mv_file(hdf, dest)
	
def mv_file(src, dest, same_fs = True):
	"""Warning:
	if src and dest are located at diff file-system, we should use:
	1. shutil.copy2(src, os.path.join(dest, src)) 
	2. os.remove(src)
	for the most safely method.
	"""
	try:
		if os.path.isfile(src):
			if same_fs:
				return shutil.move(src, dest)
			else:
				shutil.copy2(src, os.path.join(dest, src))
				os.remove(src)
		else:
			pass
	except:
		print 'mv_file error. src=' + src + ', dest=' + dest

def rm_file(file):
	try:
		if os.path.isfile(file):
			os.remove(file)
	except:
		pass
	
# get all hdf input about l1b, do NOT check file existence.
def get_hdf_l1b(l1b, ins_conf):
	hdf = {'l0': None, 'l1b': l1b, 'l1b_obc': None, 'l1b_geo': None}
	
	(filename, year, mon) = get_filename_year_mon(l1b, ins_conf)
	filepath = os.path.split(l1b)[0]
	
	if ins_conf.L0:
		hdf['l0'] = filepath + '/' + ins_conf.L0%(year, mon)

	if ins_conf.L1B_OBC:
		hdf['l1b_obc'] = filepath + '/' + ins_conf.L1B_OBC%(year, mon)

	if ins_conf.L1B_GEO:
		hdf['l1b_geo'] = filepath + '/' + ins_conf.L1B_GEO%(year, mon)

	return hdf

# check all hdf input about l1b are ok.
def hdf_l1b_ok(l1b, ins_conf):
	hdf = get_hdf_l1b(l1b, ins_conf)
	
	result = map( check_file_exist, hdf.values())
	for one_exist in result:
		if not one_exist:
			return False
		
	return True
 









