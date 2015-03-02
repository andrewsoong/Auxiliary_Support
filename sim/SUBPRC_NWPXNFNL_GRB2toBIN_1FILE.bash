#!/bin/bash -f

### INPUT....
### $1: grib2-formatted NCEP-FNL file with absolute dir
### $2: dir for binnary-formatted NCEP-FNL file

if [[ ! ( $1 && $2 ) ]] ; then
  echo "USAGE:   "
  echo "bash SUBPRC_NWPXNFNL_GRB2toBIN_1FILE.bash NFNL-filname.grb2 NFNL-dir"
  exit 1
fi
 
cmd_fix="SUBPRC_FNLX_HDF5toBIN"

cat > ${cmd_fix}.ncl << END_NCL
;------------------------------------------------------------------------
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "${NCARG_ROOT}/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "${NCARG_ROOT}/lib/UDF.ncl"
;------------------------------------------------------------------------
  begin
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues
    
    fil_in1    = "$1"
    dir_out    = "$2"
    print(fil_in1)
    fil_suf    = systemfunc("basename "+fil_in1+" | rev | gawk -F. '{print \$1}' | rev " )
    fil_fix    = systemfunc("basename "+fil_in1+" ."+fil_suf)
    print(fil_fix)
    fil_out    = dir_out+"/"+fil_fix+".DAT"
    print(fil_out)

    if ( isfilepresent(fil_out) ) then
      print("presented......"+fil_out)
      exit
      ;system("rm -rf "+fil_out)
    end if

    f_var    = addfile(fil_in1, "r")
    ;print(f_var)
    ;exit

    xlon     = f_var->lon_0
;    print(xlon)
    xlat     = f_var->lat_0
;    print(xlat)
    xplv     = f_var->lv_ISBL0
;   print(xplv/100)
;    exit

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
;      ;printMinMax(SHUM, True)
;      print(SHUM(:, 0, 0))
    fbindirwrite( fil_out, SHUM )
;      exit
    
  end 
  
END_NCL
 
ncl ${cmd_fix}.ncl #> .todelete
rm -rf ${cmd_fix}.ncl
 