ls /home/f3cmon/DATA/INPUT/HDF/obc/FY3C_MWTSX_GBAL* >/tmp/121.txt
sed 's/^/\>/g' /tmp/121.txt >/tmp/212.txt
sed 's/$/\.OK/g' /tmp/212.txt >/tmp/121.txt
cd /home/f3cmon/DATA/INPUT/HDF/obc/
sh /tmp/121.txt