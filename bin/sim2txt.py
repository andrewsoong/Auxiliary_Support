#/usr/bin/env python

"""
create by gumeng.
    translate sim's bin result to txt
"""

import struct
import sys
import os

if (len(sys.argv) != 5):
    print 'cmd parms wrong! sim2txt.py fy3b mwts input.name.dat output.name.txt'
    sys.exit(1)

sat = sys.argv[1].upper()
ins = sys.argv[2].upper()
bin_file = sys.argv[3]
txt_file = sys.argv[4]

if not os.path.exists(bin_file):
    print 'input bin.dat %s NOT exist!'%bin_file
    sys.exit(2)

sys.path.append('/home/fymonitor/MONITOR/py/etc')
conf = __import__('conf')
common = __import__('common')

# sim result bin fmt. both rttov and crtm fmt MUST NOT be difference.
# one record = 38 int[4] + 2 nwp file time [double,8] + 2 nwp coef [float,4]
sim_fmt = '=' + 'i'*38 #+ 'd'*2 + 'f'*2


txt_output = open(txt_file, 'w+')
data_cnt = 0

record_size = struct.calcsize(sim_fmt)
for one_record in common.get_bin_record_from_sim(bin_file, record_size):
    data = struct.unpack(sim_fmt, one_record)
    str_data = ','.join( map(common.data_to_str, data) ) + '\n'
    txt_output.writelines(str_data)
    data_cnt = data_cnt + 1

txt_output.close()

print '[%s] %s: read.cnt.from.hdf=%s. %s: trans.to.txt.cnt=%s'%(common.utc_nowtime(), bin_file, data_cnt, txt_file, data_cnt)




