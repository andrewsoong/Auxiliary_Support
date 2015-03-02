#!/bin/bash 

##==================================================================================================
##  OBJECT:
##    To get the correspoding forward file of inputed L1X file. 
##
##  INPUT:
##    1) L1X file with absolute directory, and some information (see bellow) could be extracted 
##       from the file name [default: command line]
##    2) ROOT dir [default: Environment vars]
##
##  OUTPUT:
##    1) FWD file
##
##==================================================================================================

if [[ $# -eq 3 ]] ; then
  DROOT=$1
  DRTMP=$2
  DRSAT=`echo ${DRTMP} | gawk -F_  '{print $1}'`
  DRIST=`echo ${DRTMP} | gawk -F_  '{print $2}'`
  DRLEV=`echo ${DRTMP} | gawk -F_  '{print $3}'`
  DRFNC=`echo ${DRTMP} | gawk -F_  '{print $4}'`
  DRRFD=`echo ${DRTMP} | gawk -F_  '{print $5}'`
  DRRTM=`echo ${DRTMP} | gawk -F_  '{print $6}'`
  DRFIN=$3
elif [[ $# -eq 1 ]] ; then
  DROOT=${MM_DIRROOT}
  DRSAT=${MM_SAT}
  DRIST=${MM_IST}
  DRFNC=${MM_FNC}
  DRRFD=${MM_RFD}
  DRRTM=${MM_RTM}
  DRFIN=$1
else 
  echo "  USAGE: "
  echo "      bash Monitor_SDR_FileInvoke.bash ROT SAT_IST_LEV_FNC_REF_RTM fil_in"
  echo "    or  "
  echo "      bash Monitor_SDR_FileInvoke.bash fil_in"
  echo "    if ENVs (ROT SAT IST LEV FNC REF RTM) are setted..  "  
  exit 1
fi
Fil_FWDFix="${DRSAT}_${DRIST}_${DRLEV}_${DRFNC}_${DRRFD}_${DRRTM}"
echo "Esentials: ${DROOT} ${DRSAT} ${DRIST} ${DRLEV} ${DRFNC} ${DRRFD}_${DRRTM}"
echo "Fil_FWDFix: ${Fil_FWDFix}" 
 
MM_DIRROOT="${DROOT}"               ; export MM_DIRROOT  ; echo "DIRROOT: ${MM_DIRROOT}"
MM_DIRSCPT="${MM_DIRROOT}/scripts"  ; export MM_DIRSCPT  ; echo "DIRSCPT: ${MM_DIRSCPT}"
MM_DIRMAIN="${MM_DIRROOT}/main"     ; export MM_DIRMAIN  ; echo "DIRMAIN: ${MM_DIRMAIN}"
MM_DIRLOGS="${MM_DIRROOT}/logs"     ; export MM_DIRLOGS  ; echo "DIRLOGS: ${MM_DIRLOGS}" 
MM_DIRFIGS="${MM_DIRROOT}/figs"     ; export MM_DIRFIGS  ; echo "DIRFIGS: ${MM_DIRFIGS}" 
MM_DIRCONT="${MM_DIRROOT}/Manger"   ; export MM_DIRCONT  ; echo "DIRCONT: ${MM_DIRCONT}" 

#### CHECK THE DIR EXISTENCE..
[[ -d ${MM_DIRROOT} ]] || { echo "DIR_ROOT: ${MM_DIRROOT} DOES NOT EXIST!" ; exit 1 ; }
[[ -d ${MM_DIRSCPT} ]] || { echo "DIR_SCPT: ${MM_DIRSCPT} DOES NOT EXIST!" ; exit 1 ; }
[[ -d ${MM_DIRMAIN} ]] || { echo "DIR_MAIN: ${MM_DIRMAIN} DOES NOT EXIST!" ; exit 1 ; }
[[ -d ${MM_DIRLOGS} ]] || mkdir -p ${MM_DIRLOGS}
[[ -d ${MM_DIRLOGS} ]] || { echo "DIR_LOGS: ${MM_DIRLOGS} DOES NOT EXIST!" ; exit 1 ; }
[[ -d ${MM_DIRFIGS} ]] || mkdir -p ${MM_DIRFIGS}
[[ -d ${MM_DIRFIGS} ]] || { echo "DIR_FIGS: ${MM_DIRFIGS} DOES NOT EXIST!" ; exit 1 ; }

## Load the functions defined.
source ${MM_DIRSCPT}/comms/MyFunctions.bash

## Get Info from input file: SAT, IST, Level FMT etc.. 
fil_in1=${DRFIN}

MM_ARG=`echo ${fil_in1} | rev | gawk -F/ '{print $7}' | rev`  #; export MM_ARG
MM_SAT=`echo ${fil_in1} | rev | gawk -F/ '{print $6}' | rev`  #; export MM_SAT
MM_IST=`echo ${fil_in1} | rev | gawk -F/ '{print $5}' | rev`  #; export MM_IST
MM_LEV=`echo ${fil_in1} | rev | gawk -F/ '{print $4}' | rev`  #; export MM_LEV
MM_FNC=`get_file_name_convention ${fil_in1} ${MM_ARG} ${MM_SAT} ${MM_IST} ${MM_LEV}`  #; export MM_FNC
MM_FMT=`get_file_format          ${fil_in1} ${MM_ARG} ${MM_SAT} ${MM_IST} ${MM_LEV}`  #; export MM_FMT
#[[ ${MM_FNC} == "NSMCL1X" ]] && { MM_FMT="TOVSL1X" ; export MM_FMT ; }
[[ ${MM_FNC} == "NSMCL1X" ]] && MM_FMT="TOVSL1X" 
echo "    ARG: ${MM_ARG} SAT: ${MM_SAT} IST: ${MM_IST} LEV: ${MM_LEV} FNC: ${MM_FNC} FMT: ${MM_FMT}"
Fil_OBSFix="${MM_SAT}_${MM_IST}_${MM_LEV}_${MM_FNC}"
echo "    Fil_OBSFix: ${Fil_OBSFix}"
#exit
 
MM_RECMP="FALSE"   #; export MM_RECMP       ## Recompile the FORTRAN CODEs
#MM_RECMP="TRUE"   #; export MM_RECMP       ## Recompile the FORTRAN CODEs
MM_REEXT="FALSE"   ; export MM_REEXT       ## Re-extract DATA  
MM_RFDS=( "${DRRFD}"     )  # ERAI T639)       # Refference datesets
MM_RTMS=( "${DRRTM}" ) #  'CRTM202'  )  # 'CRTM202' ) #  'RTTOV101'  'RTTOV07' ) 
 
## Invoke Environment for MM_SAT
source ${MM_DIRSCPT}/comms/Environment_${MM_SAT}.bash
 
EX_FMTX=${EXE1_FMTX:-"NSMCSLF"}
echo "  File Format in Execution: ${EX_FMTX}"
#exit
 
for RFD in ${MM_RFDS[*]} ; do           ## Reffrence data.
  MM_RFD=${RFD}    #; export MM_RFD
 
  for RTM in ${MM_RTMS[*]}; do          ## RTMs
    MM_RTM=${RTM}  #; export MM_RTM
 
    Fil_FWDFix="${MM_SAT}_${MM_IST}_${MM_LEV}_${MM_FNC}_${MM_RFD}_${MM_RTM}"
    echo ${MM_RTM} ${MM_RFD} ${MM_IST} ${MM_SAT} ${MM_LEV}
    echo "Fil_FWDFix: ${Fil_FWDFix}"
#    exit
    
## Get Scan info for SAT/IST    
    eval NPNT="\$NPNT${MM_IST}"
    eval NCHN="\$NCHN${MM_IST}"
    eval NADD="\$NADD${MM_SAT}"
    eval NBIT="\$NBIT${MM_SAT}"
    let "NLEN=${NCHN} + ${NADD}"
    echo "    NPNT: ${NPNT} NCHN: ${NCHN} NADD: ${NADD} NBIT: ${NBIT} NLEN: ${NLEN}"

## Files for gathered file information. 
    fexe_l1d="${MM_DIRLOGS}/L1DFILES_MSDR_${Fil_FWDFix}.txt"
    fexe_fwd="${MM_DIRLOGS}/FWDFILES_MSDR_${Fil_FWDFix}.txt"
    fexe_nwp="${MM_DIRLOGS}/NWPFILES_MSDR_${Fil_FWDFix}.txt"
    echo "    fexe_l1d: " ${fexe_l1d}
    echo "    fexe_fwd: " ${fexe_fwd}
    echo "    fexe_nwp: " ${fexe_nwp}
#    exit
    
## get the time info of inputted file  
    echo "Get_ymd_hm_${MM_SAT} ${fil_in1} ${MM_SAT} ${MM_IST} ${MM_LEV} ${MM_FNC}"
    ymd_hm=`Get_ymd_hm_${MM_SAT} ${fil_in1} ${MM_SAT} ${MM_IST} ${MM_LEV} ${MM_FNC}`
    echo ${ymd_hm} 
#    exit

    ymdmid=`echo ${ymd_hm} | gawk -F_ '{print $1}'`
    hmxmid=`echo ${ymd_hm} | gawk -F_ '{print $2}'`
    cyer=`echo ${ymdmid} | cut -c 1-4`
    cmon=`echo ${ymdmid} | cut -c 5-6`
    cday=`echo ${ymdmid} | cut -c 7-8`
    echo ${cyer} ${cmon} ${cday}
    #exit

## Get L1 data and name fwd-file
    fil_fix=`basename ${fil_in1} .DAT`
    fil_fix=`basename ${fil_fix} .HDF`
    fil_fix=`basename ${fil_fix} .BIN`
    fil_fix=`basename ${fil_fix} .L1c`
    echo ${fil_fix} 
#    exit

#### Get the name of L1D-File
    dir_l1d="${DIR1_SAT}/${MM_IST}/${dir_fix}${cyer}/${cmon}"
    [[ -d ${dir_l1d} ]] || mkdir -p ${dir_l1d}
    [[ -d ${dir_l1d} ]] || { echo "    FAILED TO Create DIR_L1D: ${dir_l1d}" ; exit 1 ; }
    fil_l1d="${dir_l1d}/${fil_fix}_OBSBTS_${EX_FMTX}.${BIN_SUFIX}"
    let "fsize1=1"
    [[ -f ${fil_l1d} && -s ${fil_l1d} ]] && fsize1=`ls -l ${fil_l1d} | gawk '{print $5}' `
    echo "${fil_l1d}  : ${fsize1}"  
        
#### Get the name of FWD-File
    dir_fwd="${DIR2_SAT}/${MM_IST}/${dir_fix}${cyer}/${cmon}"
    [[ -d ${dir_fwd} ]] || mkdir -p ${dir_fwd}
    [[ -d ${dir_fwd} ]] || { echo "      FAILED TO Create DIR_FWD: ${dir_fwd}" ; exit 1 ; } 
    fix_fwd="${dir_fwd}/${fil_fix}"
    fil_fwd="${fix_fwd}_FWDBTS_${MM_RFD}_${MM_RTM}_${EX_FMTX}.${BIN_SUFIX}"

    [[ -f ${fil_fwd} && -s ${fil_fwd} ]] &&  \
         { rm -rf ${fil_fwd} ; echo "    Delete existed FWD: ${fil_fwd}" ; }

    let "fsize2=2"
    [[ -f ${fil_fwd} && -s ${fil_fwd} ]] && fsize2=`ls -l ${fil_fwd} | gawk '{print $5}' `
    echo "${fil_fwd}  : ${fsize2}"
#    exit
     
#### To delete l1d and fwd file, if they are not of the same size
    [[ ${fsize1} -ne ${fsize2} && -f ${fil_l1d} && -s ${fil_l1d} && -f ${fil_fwd} && -s ${fil_fwd} ]] &&  \
         { rm -rf ${fil_l1d} ; rm -rf ${fil_fwd} ; echo "    Delete L1D: ${fil_l1d} and ${fil_fwd}" ; }
    
#### Get l1d file of specific format, Convertation is invoked if needed.
    if [[ ! -f ${fil_l1d} && ! -s ${fil_l1d} ]] ; then 
      echo "    "
      echo "    The L1x-file is of ${MM_FMT}-Formatted, convert to ${EX_FMTX}-formatted!"
      echo "    Begin for ${ymdmid} ...."
      CMDSH="${MM_DIRSCPT}/procs/SUBPRC_${MM_SAT}${MM_IST}_${MM_LEV}_${MM_FMT}to${EX_FMTX}.bash"
      [[ -f ${CMDSH} ]] || CMDSH="${MM_DIRSCPT}/procs/SUBPRC_SATXISTX_${MM_LEV}_${MM_FMT}to${EX_FMTX}.bash"
      echo ${CMDSH} ${MM_DIRROOT} ${Fil_OBSFix} ${fil_in1} ${fil_l1d} 
      bash ${CMDSH} ${MM_DIRROOT} ${Fil_OBSFix} ${fil_in1} ${fil_l1d}  \
           && echo "    SUCCEED IN EXECUTE ${CMDSH}" || { echo "    ERROR IN EXECUTE ${CMDSH}" ; exit 1 ; }
    else
      echo "      File exist. No need to convert!"
    fi
#    exit
    
#### exit from the script if fwd-file exist
    #[[ -f ${fil_fwd} && -s ${fil_fwd} ]] && { echo "      FWD-File exist. " ; continue ; } 
    [[ -f ${fil_fwd} && -s ${fil_fwd} ]] && { echo "      FWD-File exist. " ;} 

#### Get the scanline number of fil_l1x
    nsiz=`ls -l ${fil_l1d} | gawk '{print $5}' `
    let "nscn=${nsiz} / ${NLEN} / ${NPNT} / ${NBIT}"
    echo "    L1D SIZE ${nsiz} NSCN ${nscn}"

    if [[ ${nscn} -gt 0 ]] ; then
      printf "%8.8d %s %8.8d %s\n" ${ymdmid} ${hmxmid} ${nscn} ${fil_l1d} > ${fexe_l1d}
      printf "%8.8d %s %8.8d %s\n" ${ymdmid} ${hmxmid} ${nscn} ${fix_fwd} > ${fexe_fwd}
    else
      echo "no sat-data"
      exit 1
    fi
#    exit
   
## to prepare nwp data
    echo "    "
    echo "    Prepare NWP for ${ymdmid} ...."
    CMDSH="${MM_DIRSCPT}/procs/SUBPRC_NWPX${MM_RFD}_AROUND1DAY_GRB2toBIN.bash"
    echo ${CMDSH} ${MM_DIRROOT} ${Fil_OBSFix} ${ymdmid}
    bash ${CMDSH} ${MM_DIRROOT} ${Fil_OBSFix} ${ymdmid}                              \
         && echo "    SUCCEED IN EXECUTE ${CMDSH}" || { echo "    ERROR IN EXECUTE ${CMDSH}" ; exit ; }
#    exit
    
## NWP for one grannule
    [[ -f ${fexe_nwp} ]] && rm -rf ${fexe_nwp}
    echo "    "
    echo "    Get NWP for grannule: ${ymd_hm}"
    CMDSH="${MM_DIRSCPT}/procs/SUBPRC_NWPX${MM_RFD}_GATBIN4GRN.bash"
    echo ${CMDSH} ${MM_DIRROOT} ${Fil_OBSFix} ${ymd_hm} ${fexe_nwp}
    bash ${CMDSH} ${MM_DIRROOT} ${Fil_OBSFix} ${ymd_hm} ${fexe_nwp}                             \
         && echo "    SUCCEED IN EXECUTE ${CMDSH}" || { echo "    ERROR IN EXECUTE ${CMDSH}" ; exit ; }    
    if [[ ! -f ${fexe_nwp} || ! -s ${fexe_nwp} ]] ; then
      echo "no sat-data"
      exit 1
    fi 
  
    MM_DIREXES="${MM_DIRROOT}/exe/${Fil_FWDFix}"        #; export MM_DIREXES  ; echo "DIREXES: ${MM_DIREXES}" 
    [[ -d ${MM_DIREXES} ]] || mkdir -p ${MM_DIREXES}
    [[ -d ${MM_DIREXES} ]] || { echo "DIR_EXES: ${MM_DIREXES} DOES NOT EXIST!" ; exit 1 ; }
#    exit
     
    fexe_fil="${MM_DIREXES}/Monitor_SDR_filist.txt"
    cat > ${fexe_fil} << END_FIL
&SETNML
SEN_NAME  = "${MM_SAT}${MM_IST}"
SEN_NCHN  = ${NCHN}
L1D_NAME  = "DSTSAT"
L1D_FMTX  = "${EX_FMTX}"
RTM_NAME  = "${MM_RTM}"
RTM_NPLV  = 101
DIR_ROOT  = "${MM_DIRROOT}"
DIR_MAIN  = "${MM_DIRMAIN}"
NWP_NAME  = "${MM_RFD}"
/

&FILNML
FIL_FWDBTS  = "${fexe_fwd}"
IIL_FWDBTS  = 411, 1, 1, 1 
FIL_OBSL1D  = "${fexe_l1d}"
IIL_OBSL1D  = 413, 1, 1, 1 
FIL_NWPATM  = "${fexe_nwp}"
IIL_NWPATM  = 415, 1, 1,  1 
/

END_FIL
 
    EXE="MONITOR_SDR"
    EXE_OUT="MM_${Fil_FWDFix}"
    if [[ ${MM_RECMP} == "TRUE" || ! -e ${MM_DIREXES}/${EXE_OUT} ]] ; then
      cd ${MM_DIRMAIN} ; ls
      make EXE=${EXE} EXE_OUT=${EXE_OUT} ROOT_DIR=${MM_DIRROOT} #> .todelete
      mv ${MM_DIRROOT}/exe/${EXE_OUT} ${MM_DIREXES}
    fi
    cd ${MM_DIREXES}
    let "donum=1"

    while [[ ${fsize1} -ne ${fsize2} && ${donum} -lt 3 ]] ; do    
      #./${EXE_OUT}     change by yuanwentian
      #time ./${EXE_OUT}  1   change by yuanwentian
      #echo "${MM_DIRCONT}/KTS_Manger.sh  ./${EXE_OUT}"
      ${MM_DIRCONT}/KTS_Manger.sh  ${MM_DIREXES}/${EXE_OUT}
      let "fsize2=2"
      [[ -f ${fil_fwd} && -s ${fil_fwd} ]] && fsize2=`ls -l ${fil_fwd} | gawk '{print $5}' `
      let donum+=1
    done
    cd ${MM_DIRSCPT} 
 
  done
  
done

