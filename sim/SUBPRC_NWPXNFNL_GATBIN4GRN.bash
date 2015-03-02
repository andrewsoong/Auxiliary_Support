#!/bin/bash 
 
ymd_hm="$1"
fils_nwp_grn="$2"
 
## Load the functions defined.
source ${MM_DIRROOT}/scripts/MyFunctions.bash
source ${MM_DIRROOT}/scripts/Environment_${MM_SAT}.bash

ymdmid=`echo ${ymd_hm} | cut -c 1-8`
## Get the data satisfied and saved in ${ftmp_nwp}
ftmp_nwp="./NWPFILES_NFNL_TMP.txt"
[[ -e ${ftmp_nwp} ]] && rm -rf ${ftmp_nwp} 
 
##  before
let "imns=1"
yyyymmdd_minusdays ${ymdmid} ${imns} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR1_NWP}/NFNL/${cyer}/${cmon}/fnl_${ymdref}*.DAT ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
done

let "ymdref=${ymdmid}"
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR1_NWP}/NFNL/${cyer}/${cmon}/fnl_${ymdref}*.DAT ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
done

let "iadd=1"
yyyymmdd_adddays ${ymdmid} ${iadd} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR1_NWP}/NFNL/${cyer}/${cmon}/fnl_${ymdref}*.DAT ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
done

fils_nwp_day_tim="fils_nwp_day_tim.txt"
[[ -f ${fils_nwp_day_tim} ]] && rm -rf ${fils_nwp_day_tim}
fils_nwp_day_fil="fils_nwp_day_fil.txt"
[[ -f ${fils_nwp_day_fil} ]] && rm -rf ${fils_nwp_day_fil}
 
cat ${ftmp_nwp} | while read fil_in ; do
  ymdnwp=`basename ${fil_in} .DAT | gawk -F_ '{print $2 }' `
  hornwp=`basename ${fil_in} .DAT | gawk -F_ '{print $3 }' `
  fcsnwp=`basename ${fil_in} .DAT | gawk -F_ '{print $4 }' `
  echo ${ymdnwp} ${hornwp} ${fcsnwp} >> ${fils_nwp_day_tim}
done
cp ${ftmp_nwp} ${fils_nwp_day_fil} 
rm -rf ${ftmp_nwp}
 
ymdexe=`echo ${ymd_hm} | cut -c  1-8`
hmxexe=`echo ${ymd_hm} | cut -c 10-14`
echo ${ymdexe} ${hmxexe}
   
cmd_fix="SUBPRC_NFNL_GRN42DAY".`date '+%Y%m%d.%H%M%S'`

cat > ${cmd_fix}.ncl << END_NCL
;------------------------------------------------------------------------
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "${NCARG_ROOT}/lib/UDF.ncl"
;------------------------------------------------------------------------
  begin
    print("hello...")
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues
    
    ymdexe=${ymdexe}
    yddexe=yyyymmdd_to_yyyyddd(ymdexe)
    yerexe=yddexe/1000
    hmxexe=${hmxexe}
    horexe=hmxexe/100
    minexe=mod(hmxexe, 100)
    ymdfrc=1.0
;    print(yerexe)
    ymdfrc=yddexe*1.0 - yerexe*1000.0 + horexe*1.0/24.0 + minexe*1.0/1440. 
    print(ymdexe+"  "+horexe+"  "+minexe+" "+yddexe+"  "+ymdfrc)    
 
    tint     = 0.75  ;; 9/24

    indout   = (/1, 2, 3/)
    indout@_FillValue  = -9999
    indout   = indout@_FillValue
    
    fils_fil = "${fils_nwp_day_tim}"
    nrow1    = numAsciiRow(fils_fil)
    ncol1    = numAsciiCol(fils_fil)
    nfil1    = nrow1
    tim_in1  = new((/nrow1, ncol1/), "integer", -9999)
    tim_in1  = asciiread(fils_fil, (/nrow1, ncol1/), "integer") 
    nwpyer   = tim_in1(:, 0)/10000
    nwpfrc   = new(nrow1, "float", 1.0e+35)
    nwpfrc   = yyyymmdd_to_yyyyddd(tim_in1(:, 0)) - yerexe*1000.0 + tim_in1(:, 1)/24.0
    diffrc   = nwpfrc - ymdfrc
    print(diffrc)
    ;exit
 
;;; to add code for fix year-jump
;    if( isleapyer(yerexe) ) then
;      toadd  = 635
;    else
;      toadd  = 636
;    end if

    iout     = -1
    tttt     = diffrc
    tttt@_FillValue = 1.0e+35
    tttt     = where(  diffrc .ge. tint*-1.0 .and. diffrc .lt. 0., tttt, tttt@_FillValue )
    if ( any( .not. ismissing(tttt) ) ) then
      indbef   = maxind( tttt )
      iout   = iout + 1
      indout(iout)  = indbef
      print(iout+"  "+indbef)
      delete(indbef)
    end if
  
    tttt     = diffrc
    tttt@_FillValue = 1.0e+35
    tttt     = where(  diffrc .ge. 0 .and. diffrc .lt. tint, tttt, tttt@_FillValue )
    if ( any( .not. ismissing(tttt) ) ) then
      indbef   = minind( tttt )
      iout   = iout + 1
      indout(iout)  = indbef
      print(iout+"  "+indbef)
            
      tttt(indbef)  = tttt@_FillValue
      delete(indbef)
      if ( any( .not. ismissing(tttt) ) ) then
        indbef   = minind( tttt )
        iout   = iout + 1
        indout(iout)  = indbef
        print(iout+"  "+indbef)
        delete(indbef)      
      end if
    end if
 
    if ( iout .ge. 0 ) then
      fils_fil = "${fils_nwp_day_fil}"
      fix_in2  = asciiread(fils_fil, -1, "string")     
      print(fix_in2(indout(:iout)))
      fils_out = fix_in2(:iout)
 
      fils_out  = sprinti("%8.8i", tim_in1(indout(:iout), 0))+" "+sprinti("%2.2i", tim_in1(indout(:iout), 1)) +" "+sprinti("%8.8i", tim_in1(indout(:iout), 0))+" "+sprinti("%5.5i", tim_in1(indout(:iout), 1)) +" "+fix_in2(indout(:iout))
 
      if ( isfilepresent( "${fils_nwp_grn}" ) ) then
        system("rm -rf ${fils_nwp_grn}" )
      end if
 
      asciiwrite ("${fils_nwp_grn}", fils_out )

    end if
 
  end    
END_NCL
 
/home/f3cmon/ncl/bin/ncl ${cmd_fix}.ncl > .todelete
rm -rf ${cmd_fix}.ncl ${ftmp_nwp} ${fils_nwp_day_tim} ${fils_nwp_day_fil}
