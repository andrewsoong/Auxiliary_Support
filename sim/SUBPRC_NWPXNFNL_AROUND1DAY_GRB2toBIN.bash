#!/bin/bash -f

ymdmid="$1"
 
## Load the functions defined.
source ${MM_DIRROOT}/scripts/MyFunctions.bash
source ${MM_DIRROOT}/scripts/Environment_${MM_SAT}.bash
 
## Get the data satisfied and saved in ${ftmp_nwp}
ftmp_nwp="./NWPFILES_NFNL_TMP.txt"
[[ -e ${ftmp_nwp} ]] && rm -rf ${ftmp_nwp} 
 
##  before
let "imns=1"
yyyymmdd_minusdays ${ymdmid} ${imns} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR0_NWP}/NFNL/${cyer}/${cmon}/fnl_${ymdref}*.grb2 ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
done

let "ymdref=${ymdmid}"
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR0_NWP}/NFNL/${cyer}/${cmon}/fnl_${ymdref}*.grb2 ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
done

let "iadd=1"
yyyymmdd_adddays ${ymdmid} ${iadd} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR0_NWP}/NFNL/${cyer}/${cmon}/fnl_${ymdref}*.grb2 ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
done
 
cyer=`echo ${ymdmid} | cut -c 1-4`
cmon=`echo ${ymdmid} | cut -c 5-6`
[[ ! -d ${DIR1_NWP}/NFNL/${cyer}/${cmon} ]] && mkdir -p ${DIR1_NWP}/NFNL/${cyer}/${cmon}
 
cmd_fix="SUBPRC_FNLX_AROUND1DAY_HDF5toBIN".`date '+%Y%m%d.%H%M%S'`

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
    
    fils_in  = asciiread("${ftmp_nwp}", -1, "string")
    nfil     = dimsizes(fils_in)
 
    times    = new((/6, nfil/), "integer", -9999)
    yndy     = (/31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31/)
    
    ymdout   = new( nfil, "integer", -9999)
    horout   = ymdout
    filin1   = new( nfil, "string", "ntst")
    
    print("Calculate Time-files")
    do ifil = 0, nfil - 1
      fil_in1  = fils_in(ifil)
      iymd     = stringtointeger( systemfunc("basename "+fil_in1+" | gawk -F_ '{print \$2}' ") )
      ihor     = stringtointeger( systemfunc("basename "+fil_in1+" | gawk -F_ '{print \$3}' ") )
      iyer     = iymd/10000
      imon     = iymd / 100 - iyer * 100
 
      fil_suf  = systemfunc("basename "+fil_in1+" | rev | gawk -F. '{print \$1}' | rev " )
      fil_fix  = systemfunc("basename "+fil_in1+" ."+fil_suf)
      dir_out  = "${DIR1_NWP}/NFNL/"+sprinti("%4.4i",iyer)+"/"+sprinti("%2.2i",imon)
      fil_out  = dir_out+"/"+fil_fix+".DAT"
      system( "mkdir -p "+ dir_out)

      fils_in(ifil)  = sprinti("%8.8i",iymd)+" "+sprinti("%2.2i",ihor)+" "+sprinti("%8.8i",iymd)+" "+sprinti("%5.5i",ihor*1000)+" "+fil_out
 
      if ( isfilepresent( fil_out ) ) then
        if( "${MM_REEXT}" .eq. "FALSE") then
          continue
        else
          system("rm -rf "+fil_out)
        end if
      end if
 
      f_var    = addfile(fil_in1, "r")
      ;print(f_var)
      ;exit

      xlon     = f_var->lon_0
;      print(xlon)
      xlat     = f_var->lat_0
;      print(xlat)
      xplv     = f_var->lv_ISBL0
;      print(xplv/100)
;      exit

;; psfc    	
      dout  = f_var->PRES_P0_L1_GLL0(::-1, :)*0.01
      psfc  = dout
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; slp
      dout  = f_var->PRMSL_P0_L101_GLL0(::-1, :)*0.01
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; tsfc
      dout  = f_var->TMP_P0_L1_GLL0(::-1, :) 
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; satx
      dout  = f_var->TMP_P0_L103_GLL0(::-1, :) 
      t2m   = dout
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; uwnd
      dout  = f_var->UGRD_P0_L103_GLL0(::-1, :) 
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout )    	

;; vwnd
      dout  = f_var->VGRD_P0_L103_GLL0(::-1, :) 
      ;printMinMax(dout, True) 
      fbindirwrite( fil_out, dout ) 

;; saq
      RH    = f_var->RH_P0_L103_GLL0(::-1, :) 
      dout  = mixhum_ptrh(psfc, t2m, RH, -2)
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout ) 

;; ZSFC
      dout  = f_var->HGT_P0_L1_GLL0(::-1, :)*0.001
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout )
       
;; tlnd
      dout  = f_var->LAND_P0_L1_GLL0(::-1, :) 
      ;printMinMax(dout, True)
      fbindirwrite( fil_out, dout )
      
;; tair
      TAIR  = f_var->TMP_P0_L100_GLL0(:,::-1, :)            
      ;printMinMax(TAIR, True)
      fbindirwrite( fil_out, TAIR )

;; h2ox
      RHUM    = f_var->RH_P0_L100_GLL0(:, ::-1, :)
      PH2O    = f_var->lv_ISBL4/100.
      PH2O2   = RHUM
      do ii = 0, 20
        PH2O2(ii, :, :)  = (/PH2O(ii)/)
      end do
      SHUM    = TAIR
      SHUM(5:, :, :)  = (/mixhum_ptrh(PH2O2, TAIR(5:, :, :), RHUM, -1 )/)  ; g/kg
      do ii = 0, 4
        SHUM(ii, :, :)  = SHUM(5, :, :) 
      end do
;      do3d  = f_var->SPFH_P0_L100_GLL0(:,::-1, :)*1000. 
      SHUM  = where(SHUM .lt. 1.0e-4, 1.0e-4, SHUM)           
;        ;printMinMax(SHUM, True)
;      print(SHUM(:, 0, 0))
      fbindirwrite( fil_out, SHUM )
;      exit
 
      print(""+fil_out)
     
    end do
    
    if ( isfilepresent( "${fils_nwp}" ) ) then
      system("rm -rf ${fils_nwp}" )
    end if
    ;print("${fils_nwp}")
    asciiwrite ("${fils_nwp}", fils_in) 
    
  end 
  
END_NCL
 
/home/f3cmon/ncl/bin/ncl ${cmd_fix}.ncl > .todelete
rm -rf ${cmd_fix}.ncl ${ftmp_nwp}
 