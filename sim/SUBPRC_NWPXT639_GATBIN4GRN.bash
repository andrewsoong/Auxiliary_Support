#!/bin/bash 
 
ymd_hm="$1"
fils_nwp_grn="$2"
 
## Load the functions defined.
source ${MM_DIRROOT}/scripts/MyFunctions.bash
source ${MM_DIRROOT}/scripts/Environment_${MM_SAT}.bash

ymdmid=`echo ${ymd_hm} | cut -c 1-8`
## Get the data satisfied and saved in ${ftmp_nwp}
ftmp_nwp="./NWPFILES_T639_TMP.txt"
[[ -e ${ftmp_nwp} ]] && rm -rf ${ftmp_nwp} 
 
##  before
let "imns=1"
yyyymmdd_minusdays ${ymdmid} ${imns} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR1_NWP}/T639/${cyer}/${cmon}/gmf.639.${ymdref}*[0-2]?.DAT ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
#  echo ${fil_in1}
done
 
let "ymdref=${ymdmid}"
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR1_NWP}/T639/${cyer}/${cmon}/gmf.639.${ymdref}*[0-2]?.DAT ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
#  echo ${fil_in1}
done

let "iadd=1"
yyyymmdd_adddays ${ymdmid} ${iadd} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR1_NWP}/T639/${cyer}/${cmon}/gmf.639.${ymdref}*[0-2]?.DAT ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
#  echo ${fil_in1}
done
 
fils_nwp_day_tim="fils_nwp_day_tim.txt"
[[ -f ${fils_nwp_day_tim} ]] && rm -rf ${fils_nwp_day_tim}
fils_nwp_day_fil="fils_nwp_day_fil.txt"
[[ -f ${fils_nwp_day_fil} ]] && rm -rf ${fils_nwp_day_fil}
 
ymdexe=`echo ${ymd_hm} | cut -c  1-8`
hmxexe=`echo ${ymd_hm} | cut -c 10-14`
#echo ${ymdexe} ${hmxexe}

for fil_in in $( cat ${ftmp_nwp} )  ; do
#  echo ${fil_in}
  ymdnwp=`basename ${fil_in} | cut -c  9-16 `
  hornwp=`basename ${fil_in} | cut -c 17-18 `
  fcsnwp=`basename ${fil_in} | rev | cut -c 5-7 | rev `
  filnwp=${fil_in}
  echo ${ymdnwp} ${hornwp} ${fcsnwp} >> ${fils_nwp_day_tim}
  echo ${filnwp} >> ${fils_nwp_day_fil}  
done
  
cmd_fix="SUBPRC_T639_GRN42DAY".`date '+%Y%m%d.%H%M%S'`

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
;    print(ymdexe+"  "+horexe+"  "+minexe+" "+yddexe+"  "+ymdfrc)    
;    exit

    tint     = 0.375  ;; 9/24

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
    nwpfrc   = yyyymmdd_to_yyyyddd(tim_in1(:, 0)) - yerexe*1000.0 + tim_in1(:, 1)/24.0 + tim_in1(:, 2)/24.0 
    diffrc   = nwpfrc - ymdfrc
;    print(diffrc)
;    exit
    
;;; to add code for fix year-jump
;    if( isleapyer(yerexe) ) then
;      toadd  = 635
;    else
;      toadd  = 636
;    end if
;    print(diffrc)
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
    
    fils_fil = "${fils_nwp_day_fil}"
    nrow2    = numAsciiRow(fils_fil)
    ncol2    = numAsciiCol(fils_fil)
    nfil2    = nrow2
    fix_in2  = asciiread(fils_fil, -1, "string")     

    fils_out  = new(3, "string", "not")
    j    = -1
    do i = 0, iout 
      if ( .not. ismissing( indout(i) ) ) then
         j = j + 1
         ;print(fix_in2(indout(i)))
         print(tim_in1(indout(i), 0)+" "+tim_in1(indout(i), 1)+" "+tim_in1(indout(i), 2))
         hhm = tim_in1(indout(i), 1) + tim_in1(indout(i), 2)
         ymd = tim_in1(indout(i), 0)
         if ( hhm .ge. 24 ) then
           ndy  = hhm/24
           hhm  = hhm - 24*ndy
           
           yer  = tim_in1(indout(i), 0)/10000
           ydd  = yyyymmdd_to_yyyyddd(tim_in1(indout(i), 0))
           ddd  = mod(ydd, 1000)
           if ( isleapyear(yer) ) then
             ydy = 366
           else
             ydy = 365
           end if
           
           if ( ddd + ndy .gt. ydy ) then
             ymd  = (tim_in1(indout(i), 0)/10000 + 1)*10000 + 100 + ddd + ndy  - ydy
           else
             ymd  = yyyyddd_to_yyyymmdd( yyyymmdd_to_yyyyddd(tim_in1(indout(i), 0)) + 1 )
           end if
         end if
         ctim1  = systemfunc("basename "+fix_in2(indout(i))+" | gawk -F. '{print \$3}' | cut -c  1-8 ")
         ctim2  = systemfunc("basename "+fix_in2(indout(i))+" | gawk -F. '{print \$3}' | cut -c 9-13 ")
         ;fils_out(j) = sprinti("%8.8i", tim_in1(indout(i), 0))+" "+sprinti("%2.2i", tim_in1(indout(i), 1))+" "+ctim+" "+fix_in2(indout(i))
         fils_out(j) = sprinti("%8.8i", ymd)+" "+sprinti("%2.2i", hhm)+" "+ctim1+" "+ctim2+" "+fix_in2(indout(i))
      end if
    end do

    if ( isfilepresent( "${fils_nwp_grn}" ) ) then
      system("rm -rf ${fils_nwp_grn}" )
    end if
    print("${fils_nwp_grn}")
    print(j+"  "+fils_out(0:j))
    if ( j .gt. -1 ) then
      asciiwrite ("${fils_nwp_grn}", fils_out(0:j))
    end if
    
  end    
END_NCL
echo ${fils_nwp_grn}
 
/home/f3cmon/ncl/bin/ncl ${cmd_fix}.ncl > .todelete
rm -rf ${cmd_fix}.ncl ${ftmp_nwp} ${fils_nwp_day_tim} ${fils_nwp_day_fil}

