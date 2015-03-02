#!/bin/bash 

## For the Original data
DIR0_ALL="/home/f3cmon/DATA"          ; export DIR0_ALL 
DIR0_NWP="${DIR0_ALL}/MODEL"          ; export DIR0_NWP
DIR0_SAT="${DIR0_ALL}/SAT/NSMC/FY3B"  ; export DIR0_SAT
DIR0_IRAS="${DIR0_SAT}/IRAS"          ; export DIR0_IRAS
DIR0_MWTS="${DIR0_SAT}/MWTS"          ; export DIR0_MWTS
DIR0_MWHS="${DIR0_SAT}/MWHS"          ; export DIR0_MWHS
DIR0_VASS="${DIR0_SAT}/VASS"          ; export DIR0_VASS
DIR0_VIRR="${DIR0_SAT}/VIRR"          ; export DIR0_VIRR

declare -i -x NCHNIRAS=20
declare -i -x NCHNMWTS=4
declare -i -x NCHNMWHS=5
declare -i -x NCHNVASS=35
declare -i -x NPNTIRAS=56
declare -i -x NPNTMWTS=15
declare -i -x NPNTMWHS=98
declare -i -x NPNTVASS=56
declare -i -x NADDFY3B=25   ## number of var of geo-inform
declare -i -x NBITFY3B=4

## For the dir suitable for FYSAT
DIR1_ALL="${DIR0_ALL}/MNTIN"; export DIR1_ALL 
DIR1_NWP="${DIR1_ALL}/NWP"          ; export DIR1_NWP
DIR1_SAT="${DIR1_ALL}/FY3B"         ; export DIR1_SAT
DIR1_IRAS="${DIR1_SAT}/IRAS"        ; export DIR1_IRAS
DIR1_MWTS="${DIR1_SAT}/MWTS"        ; export DIR1_MWTS
DIR1_MWHS="${DIR1_SAT}/MWHS"        ; export DIR1_MWHS 
DIR1_VASS="${DIR1_SAT}/VASS"        ; export DIR1_VASS 
DIR1_VIRR="${DIR1_SAT}/VIRR"        ; export DIR1_VIRR
 
## For the dir for output
DIR2_ALL="${DIR0_ALL}/MNTOUT" ; export DIR2_ALL
DIR2_SAT="${DIR2_ALL}/FY3B"          ; export DIR2_SAT 
DIR2_IRAS="${DIR2_SAT}/IRAS"         ; export DIR2_IRAS   ## FWD IR1
DIR2_MWTS="${DIR2_SAT}/MWTS"         ; export DIR2_MWTS   ## FWD MWT
DIR2_MWHS="${DIR2_SAT}/MWHS"         ; export DIR2_MWHS   ## FWD MWH
DIR2_VASS="${DIR2_SAT}/VASS"         ; export DIR2_VASS   ## FWD VAS
DIR2_RTVS="${DIR2_SAT}/RTVS"         ; export DIR2_RTVS   ## Retrievals


function Get_ymd_hm_FY3BHDF {
  local in2=$2
  local ymd_hm
  
  ymd_hm=`basename $1 | rev | cut -c 14-26 | rev`

  if [[ "$in2" ]]; then
    $2="$ymd_hm" 
  else 
    echo "$ymd_hm"
  fi 
}


function Get_ymd_hm_FY3B {
  local in2=$2
  local ymd_hm
  
  ymd=`basename $1 | gawk -F_ '{print $5}' | cut -c  1-8`
  hmx=`basename $1 | gawk -F_ '{print $5}' | cut -c 9-12`
  ymd_hm="${ymd}_${hmx}"

  if [[ "$in2" ]]; then
    $2="$ymd_hm" 
  else 
    echo "$ymd_hm"
  fi 
}
