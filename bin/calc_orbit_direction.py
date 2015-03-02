#/usr/bin/env python

"""
create by gumeng.
calculate orbit direction for one table.
direction in DB is :
obt_direct = 1 = ascend
obt_direct = 2 = descend.
"""

from datetime import timedelta
import h5py as h5
import MySQLdb
import time
import sys
import os

if (len(sys.argv) != 4 and len(sys.argv) != 5):
	print "cmd parms wrong! calc_oneday_global.py satellite instrument table"
	sys.exit(1)

sat = sys.argv[1].upper()
ins = sys.argv[2].upper()
table = sys.argv[3].upper()

sys.path.append('/home/fymonitor/MONITOR/py/etc')
conf = __import__('conf')
common = __import__('common')

ins_conf_file = sat + '_' + ins + '_CONF'
ins_conf = __import__(ins_conf_file)

channels = ins_conf.channels
pixels = 254#ins_conf.pixels

time_begin = time.time()
try:
    conn=MySQLdb.connect(host=conf.db_setting['master']['ip'], 
                         user=conf.db_setting['master']['user'],
                         passwd=conf.db_setting['master']['pwd'], 
                         port=conf.db_setting['master']['port'])
    cur=conn.cursor()
    conn.select_db('FY3C_MWRI')
    sql = 'select scln, lat from ' + table + ' group by scln '
    cur.execute(sql)
    data = cur.fetchall()
except MySQLdb.Error, e:
    print "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(3)

scans = len(data)
if scans <= 1:
	print 'only one scans, can NOT cal orbit direction'
	sys.exit(1)
	
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

sql_update = 'update ' + table + ' set obt_direct=%s where scln=%s'
sql_update_between = 'update ' + table + ' set obt_direct=%s where scln between %s and %s '

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
	sql = sql_update_between%(previous_direction, begin_scan, one_scan)
	print sql
	try:
	    cur.execute(sql)
	except MySQLdb.Error, e:
	    print "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
	    sys.exit(3)
	# update flag
	previous_direction = cur_direction
	begin_scan = one_scan + 1

# update the last scan
sql_last = sql_update_between%(previous_direction, begin_scan, scans)  #sql_update%(previous_direction, scans)
print sql_last

try:
	cur.execute(sql_last)
except MySQLdb.Error, e:
    print "Mysql Fatal Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(3)

conn.commit()
cur.close()
conn.close()

time_end = time.time()
print 'insert to db by [one_data]*100, timeuse ' \
	+ str(round(time_end - time_begin, 2))


