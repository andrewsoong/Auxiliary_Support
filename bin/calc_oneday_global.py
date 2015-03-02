#/usr/bin/env python

"""
create by gumeng.
calculate obs-sim, etc. for one day global.
Do NOT insert result to mysql.

eg:
if we are crontabed at 2013-12-06 12:00, we should calc for previous time zone: 
[2013-12-05 00:00, 2013-12-05 23:59]

cron time:		12 [every 24 hours]
---------------------------------------------------------------
calc for:  [yesterday]00:00---23:59

"""

import h5py as h5
import MySQLdb
import time
import sys
import os

if (len(sys.argv) != 4 and len(sys.argv) != 5):
	print "cmd parms wrong! calc_oneday_global.py satellite instrument [year-mon-day]"
	print "year-mon-day: set the ymd you want calc for. usefull when redo. eg: 2013-12-06."
	print "			  ignored if calc for cur time."
	sys.exit(1)

sat = sys.argv[1].upper()
ins = sys.argv[2].upper()

if (len(sys.argv) == 4):
	ymd = sys.argv[3]
else:
	pass
	ymd = '2013-12-01'
	# calc for cur time

timeArray = time.strptime(ymd, "%Y-%m-%d")
timeStamp = time.mktime(timeArray)
ymd = time.strftime("%Y%m%d", timeArray)
	
sys.path.append('/home/fymonitor/MONITOR/py/etc')
conf = __import__('conf')
common = __import__('common')

ins_conf_file = sat + '_' + ins + '_CONF'
ins_conf = __import__(ins_conf_file)

channels = ins_conf.channels
pixels = ins_conf.pixels

# if not ins_conf.need_calc_stat_hourly:
# 	print 'For ' + sat + ' ' + ins + ' setting, do NOT need calc stat hourly!'
# 	sys.exit(1)
	
#get the correct tables.
"""
We MUST create fy3b-mwts table's info, for easy time search
"""
try:
    conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                         user=conf.db_setting['master']['user'],
                         passwd=conf.db_setting['master']['pwd'], 
                         port=conf.db_setting['master']['port'])
    cur=conn.cursor()
    conn.select_db(ins_conf.data_db)
    cur.execute('show tables') # the result is already sorted by ascii.
    all_tables = cur.fetchall()
except MySQLdb.Error, e:
    print "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(3)

my_table = []
for one_table in all_tables:
	table_ymd = one_table[0][19:27] # FY3B_MWTSX_GBAL_L1_20131123_0501_060KM_MS
	if not cmp(ymd, table_ymd):
		my_table.extend([one_table[0]])

if len(my_table)<=0:
	print 'no table found for time ' + ymd + ', please reset time span.'
	sys.exit(4)

# create sql like:
# select  sim_mod, lat, lon, obs_bt1, obs_bt2, obs_bt3, obs_bt4, sim_bt1, sim_bt2, sim_bt3, sim_bt4,
# obs_bt1-sim_bt1, obs_bt2-sim_bt2, obs_bt3-sim_bt3, obs_bt4-sim_bt4 from ( 
# select sim_mod, lat, lon, obs_bt1, sim_bt1, obs_bt2, sim_bt2, obs_bt3, sim_bt3, obs_bt4, sim_bt4 
# from FY3B_MWTSX_GBAL_L1_20131110_0220_060KM_MS 
# union all select sim_mod, lat, lon, obs_bt1, sim_bt1, obs_bt2, sim_bt2, obs_bt3, sim_bt3, obs_bt4, 
# sim_bt4 from FY3B_MWTSX_GBAL_L1_20131110_1048_060KM_MS ) as total limit 2;
sql = 'select lat, lon, obs_bt1, obs_bt2, obs_bt3, obs_bt4, ' + \
	'sim_bt1, sim_bt2, sim_bt3, sim_bt4, ' + \
	'obs_bt1-sim_bt1, obs_bt2-sim_bt2, obs_bt3-sim_bt3, obs_bt4-sim_bt4 from ( ' 
sub_select = 'select lat, lon, obs_bt1, sim_bt1, obs_bt2, sim_bt2, obs_bt3, sim_bt3, obs_bt4, sim_bt4 '
sub_where_rttov = ' where sim_mod=1 '
sub_where_crtm = ' where sim_mod=2 '
total = ' ) as total'

sql_rttov = sql + ' union all '.join(map(lambda x,y,z: x + ' from ' + y + ' ' + z, \
			[sub_select]*len(my_table), my_table, [sub_where_rttov]*len(my_table) ) ) + total
			
sql_crtm = sql + ' union all '.join(map(lambda x,y,z: x + ' from ' + y + ' ' + z, \
			[sub_select]*len(my_table), my_table, [sub_where_crtm]*len(my_table) ) ) + total

try:
    conn.select_db(ins_conf.data_db)
    cur.execute(sql_rttov)
    rttov_data = cur.fetchall()
    cur.execute(sql_crtm)
    crtm_data = cur.fetchall()
except MySQLdb.Error, e:
    print "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(3)

cur.close()
conn.close()

scans = len(rttov_data)/pixels # pixels=15

filename = './' + sat + '_' + ins + '_GLOBAL_' + ymd + '.HDF'
hdf = h5.File(filename, 'w')
#lat = hdf.create_dataset("lat", (scans, pixels), dtype='f',  maxshape=(None, None))
lat = hdf.create_dataset("lat", (scans, pixels), dtype='f')
lon = hdf.create_dataset("lon", (scans, pixels), dtype='f')
obs_bt = hdf.create_dataset("obs_bt", (channels, scans, pixels), dtype='f')
sim_bt_rttov = hdf.create_dataset("sim_bt_rttov", (channels, scans, pixels), dtype='f')
diff_value_rttov = hdf.create_dataset("diff_rttov", (channels, scans, pixels), dtype='f')
sim_bt_crtm = hdf.create_dataset("sim_bt_crtm", (channels, scans, pixels), dtype='f')
diff_value_crtm = hdf.create_dataset("diff_crtm", (channels, scans, pixels), dtype='f')

for one_scan in xrange(0, scans):
	# extra pixels data one-time. one for rttov, another for crtm.
	one_scan_rttov = rttov_data[one_scan*pixels : (one_scan+1)*pixels]
	one_scan_crtm = crtm_data[one_scan*pixels : (one_scan+1)*pixels]
	for i in xrange(0, pixels):
		lat[one_scan, i] = one_scan_rttov[i][0]
		lon[one_scan, i] = one_scan_rttov[i][1]
		for j in xrange(0, channels):
			if type(None) != type(one_scan_rttov[i][2+j]):
				obs_bt[j, one_scan, i] = one_scan_rttov[i][2+j]/100.0
			if type(None) != type(one_scan_rttov[i][6+j]):
				sim_bt_rttov[j, one_scan, i] = one_scan_rttov[i][6+j]/100.0
			if type(None) != type(one_scan_rttov[i][10+j]):
				diff_value_rttov[j, one_scan, i] = one_scan_rttov[i][10+j]/100.0
			else:
				diff_value_rttov[j, one_scan, i] = obs_bt[j, one_scan, i] - sim_bt_rttov[j, one_scan, i]
			if type(None) != type(one_scan_crtm[i][6+j]):
				sim_bt_crtm[j, one_scan, i] = one_scan_crtm[i][6+j]/100.0
			if type(None) != type(one_scan_crtm[i][10+j]):
				diff_value_crtm[j, one_scan, i] = one_scan_crtm[i][10+j]/100.0
			else:
				diff_value_crtm[j, one_scan, i] = obs_bt[j, one_scan, i] - sim_bt_crtm[j, one_scan, i]

hdf.close()

print 'calc one day global for ' + sat + ' ' + ins + ' ' + ymd + ' ok. write ' + \
	str(scans) + ' scans data to ' + filename





