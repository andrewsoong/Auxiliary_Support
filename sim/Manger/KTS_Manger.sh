#PBS -l nodes=10:ppn=10

echo "TORQUE: Start at "`date`

#mpdboot -n `wc /COMM1/wucq/FYMONITOR4SVN/Manger/nodelist |awk '{print $1}'` -r ssh -f /COMM1/wucq/FYMONITOR4SVN/Manger/nodelist > mpdboot.log

/opt/intel/impi/4.1.0/intel64/bin/mpiexec  -genv -ppn 24 -n 24 /home/fymonitor/MONITORFY3C/Manger/KTS_cluster_manger.exe  $1 

echo "TORQUE: End at "`date`
