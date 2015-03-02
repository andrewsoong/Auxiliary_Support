#!/bin/bash 

PCNAME=`hostname`
GPNAME=`groups`
IDNAME=`whoami`
DIRID="${GPNAME}${IDNAME}"
case "${DIRID}" in
ldaswucq)            ## For Server Luqifeng, Dell
  DIR0_ALL="/data/FYMNT"                  ; export DIR0_ALL
  DIR1_ALL="/data/wucq/MONITOR/MNTIN"     ; export DIR1_ALL
  DIR2_ALL="/data/wucq/MONITOR/MNTOUT"    ; export DIR2_ALL
;;
operoper1)            ## For Server Luqifeng, Dell
  DIR0_ALL="/data/FYMNT"                  ; export DIR0_ALL
  DIR1_ALL="/data/FYMNT/MNTIN"            ; export DIR1_ALL
  DIR2_ALL="/data/FYMNT/MNTOUT"           ; export DIR2_ALL
;;
expertexpert05)
  DIR0_ALL="/data/FYMNT"                  ; export DIR0_ALL
  DIR1_ALL="/data/expert05/parallel/MNTIN"  ; export DIR1_ALL
  DIR2_ALL="/data/expert05/parallel/MNTOUT" ; export DIR2_ALL
;;
FY4wucq)      ## For Server Jun Li, Dell  
  DIR0_ALL="/huawei2/DATA"                ; export DIR0_ALL
  DIR0_PDB="/mnt/dat/sdb1/wucq/PDBRES"          ; export DIR0_PDB
  DIR1_ALL="/mnt/dat/sdb1/wucq/RTVIN"           ; export DIR1_ALL
  DIR2_ALL="/mnt/dat/sdb1/wucq/RTVOUT"          ; export DIR2_ALL
;;
FY4operun)    ## For Server Jun Li, Dell  
  DIR0_ALL="/huawei2/DATA"                ; export DIR0_ALL
  DIR1_ALL="/huawei/OPERUN/RTVIN"         ; export DIR1_ALL
  DIR2_ALL="/huawei/OPERUN/RTVOUT"        ; export DIR2_ALL
  ;;
*)
  echo "No Server Matched, use default of /home/f3cmon/DATA !"
;;
esac

BIN_SUFIX="bin"                       ; export BIN_SUFIX
declare -i -x NDAY2CHK=2
declare -i -x BL_YMD=20131001 
declare -i -x BU_YMD=`date +%Y%m%d`
declare -i -x BTMAX=40000
declare -i -x BTMIN=5000

## For the Original data
DIR0_ALL="/home/f3cmon/DATA"          ; export DIR0_ALL 
DIR0_NWP="${DIR0_ALL}/NWP"           ; export DIR0_NWP
DIR0_SAT="${DIR0_ALL}/SAT/NSMC/FY3C" ; export DIR0_SAT
DIR0_IRAS="${DIR0_SAT}/IRAS"         ; export DIR0_IRAS
DIR0_MWTS="${DIR0_SAT}/MWTS"         ; export DIR0_MWTS
DIR0_MWHS="${DIR0_SAT}/MWHS"         ; export DIR0_MWHS
DIR0_VASS="${DIR0_SAT}/VASS"         ; export DIR0_VASS
DIR0_MWRI="${DIR0_SAT}/MWRI"         ; export DIR0_MWRI
DIR0_VIRR="${DIR0_SAT}/VIRR"         ; export DIR0_VIRR 
DIR0_VHTM="${DIR0_SAT}/VTHM"         ; export DIR0_VHTM 

## For the dir suitable for FYSAT
DIR1_ALL="${DIR0_ALL}/MNTIN"        ; export DIR1_ALL 
DIR1_NWP="${DIR1_ALL}/NWP"            ; export DIR1_NWP
DIR1_SAT="${DIR1_ALL}/SAT/NSMC/FY3C"  ; export DIR1_SAT
DIR1_IRAS="${DIR1_SAT}/IRAS"          ; export DIR1_IRAS
DIR1_MWTS="${DIR1_SAT}/MWTS"          ; export DIR1_MWTS
DIR1_MWHS="${DIR1_SAT}/MWHS"          ; export DIR1_MWHS
DIR1_MWRI="${DIR1_SAT}/MWRI"          ; export DIR1_MWRI
DIR1_VASS="${DIR1_SAT}/VASS"          ; export DIR1_VASS
DIR1_VIRR="${DIR1_SAT}/VIRR"          ; export DIR1_VIRR
EXE1_FMTX="TOVSL1X"                   ; export EXE1_FMTX

## NSMCSLF: Self-defined data sequence.
## TOVSL1X: data sequence for NWP model, internationally used.
## NSMCASK: ASCII data

## For the dir for output
DIR2_ALL="${DIR0_ALL}/MNTOUT"        ; export DIR2_ALL
DIR2_SAT="${DIR2_ALL}/SAT/NSMC/FY3C"  ; export DIR2_SAT 
DIR2_IRAS="${DIR2_SAT}/IRAS"          ; export DIR2_IRAS   ## FWD IRAS
DIR2_MWTS="${DIR2_SAT}/MWTS"          ; export DIR2_MWTS   ## FWD MWTS
DIR2_MWHS="${DIR2_SAT}/MWHS"          ; export DIR2_MWHS   ## FWD MWHS
DIR2_MWRI="${DIR2_SAT}/MWRI"          ; export DIR2_MWRI   ## FWD MWRI
DIR2_VASS="${DIR2_SAT}/VASS"          ; export DIR2_VASS   ## FWD VASS
DIR2_RTVS="${DIR2_SAT}/RTVS"          ; export DIR2_RTVS   ## Retrievals
DIR2_FIGS="${DIR2_SAT}/FIGS"          ; export DIR2_FIGS   ## FIGs

declare -i -x NCHNIRAS=26
declare -i -x NCHNMWTS=13
declare -i -x NCHNMWHS=15
declare -i -x NCHNMWRI=10
declare -i -x NCHNVASS=54
declare -i -x NCHNVIRR=1
declare -i -x NPNTIRAS=56
declare -i -x NPNTMWTS=90
declare -i -x NPNTMWHS=98
declare -i -x NPNTMWRI=254
declare -i -x NPNTVASS=56
declare -i -x NPNTVIRR=56
[[ ${EXE1_FMTX} == "NSMCSLF" ]] && declare -i -x NADDFY3C=23   ## number of var of geo-inform
[[ ${EXE1_FMTX} == "TOVSL1X" ]] && declare -i -x NADDFY3C=25   ## number of var of geo-inform
declare -i -x NBITFY3C=4
