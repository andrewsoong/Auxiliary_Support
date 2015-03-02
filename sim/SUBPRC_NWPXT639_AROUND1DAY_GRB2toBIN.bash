#!/bin/bash -f

ymdmid="$1"
 
## Load the functions defined.
source ${MM_DIRROOT}/scripts/MyFunctions.bash
source ${MM_DIRROOT}/scripts/Environment_${MM_SAT}.bash
 
## Get the data satisfied and saved in ${ftmp_nwp}
ftmp_nwp="./NWPFILES_T639_TMP.txt"
[[ -e ${ftmp_nwp} ]] && rm -rf ${ftmp_nwp} 
 
##  before
let "imns=1"
yyyymmdd_minusdays ${ymdmid} ${imns} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR0_NWP}/T639/${cyer}/${cmon}/gmf.639.${ymdref}*[0-2]?.grb2 ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
#  echo ${fil_in1}
done
 
let "ymdref=${ymdmid}"
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR0_NWP}/T639/${cyer}/${cmon}/gmf.639.${ymdref}*[0-2]?.grb2 ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
#  echo ${fil_in1}
done

let "iadd=1"
yyyymmdd_adddays ${ymdmid} ${iadd} ymdref
#echo "reftim: "  ${ymdref} 
cyer=`echo ${ymdref} | cut -c 1-4`
cmon=`echo ${ymdref} | cut -c 5-6`
for fil_in1 in $(ls ${DIR0_NWP}/T639/${cyer}/${cmon}/gmf.639.${ymdref}*[0-2]?.grb2 ) ; do
  echo ${fil_in1} >> ${ftmp_nwp}
#  echo ${fil_in1}
done
 
cyer=`echo ${ymdmid} | cut -c 1-4`
cmon=`echo ${ymdmid} | cut -c 5-6`
[[ ! -d ${DIR1_NWP}/NFNL/${cyer}/${cmon} ]] && mkdir -p ${DIR1_NWP}/NFNL/${cyer}/${cmon}


cmd_fix="SUBPRC_T639_HDF5toBIN".`date '+%Y%m%d.%H%M%S'`

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
;    print(fils_in)
;    exit
    
    times    = new((/6, nfil/), "integer", -9999)
    yndy     = (/31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31/)
    
    ymdout   = new( nfil, "integer", -9999)
    horout   = ymdout
    filin1   = new( nfil, "string", "ntst")
    
    print("Calculate Time-files")
    do ifil = 0, nfil - 1
      fil_in   = fils_in(ifil)
      ymdh     = stringtointeger( systemfunc("basename "+fil_in+" | cut -c  9-18") )
      iyer     = stringtointeger( systemfunc("basename "+fil_in+" | cut -c  9-12") )
      imon     = stringtointeger( systemfunc("basename "+fil_in+" | cut -c 13-14") )
      iday     = stringtointeger( systemfunc("basename "+fil_in+" | cut -c 15-16") )
      ihor     = stringtointeger( systemfunc("basename "+fil_in+" | cut -c 17-18") )
      hfst     = stringtointeger( systemfunc("basename "+fil_in+" | cut -c 19-21") )
      ihor     = ihor + hfst

      if ( isleapyear(iyer) ) then
        yndy(1) = 29
      else
        yndy(1) = 28
      end if
      
      if ( ihor .ge. 48 ) then
        ihor   = ihor - 48
        iday   = iday + 2
      end if      
      
      if ( ihor .ge. 24 ) then
        ihor   = ihor - 24
        iday   = iday + 1
      end if

      if ( iday .gt. yndy(imon - 1) )
        iday   = iday - yndy(imon - 1)
        imon   = imon + 1
      end if

      if ( imon .gt. 12 ) then
        imon = imon - 12
        iyer = iyer + 1
      end if 
      
      times(0, ifil) = iyer
      times(1, ifil) = imon
      times(2, ifil) = iday
      times(3, ifil) = ihor
      times(4, ifil) = iyer*1000000 + imon*10000 + iday*100 + ihor
      times(5, ifil) = hfst     
;      print(ymdh+"  "+hfst+"  "+iyer+"  "+imon+"  "+iday+"  "+ihor )
;      print((/times(:, ifil)/))
    end do        

    print("Kick off the duplicated files:  ")
    iget  = 0
    do while ( any( .not. ismissing( times(4, :) ) ) )
      indmin  = ind( times(4, :) .eq. min(times(4, :)) )
      nmin    = dimsizes(indmin)
      iii     = indmin(nmin - 1)
      ymdout(iget) = times(0, iii)*10000 + times(1, iii)*100 + times(2, iii) 
      horout(iget) = times(3, iii)
      filin1(iget) = fils_in(iii)
      iget    = iget + 1
      times(4, indmin)  = times@_FillValue
      delete(indmin)
    end do
    print(ymdout(:iget-1)+"  "+horout(:iget-1)+"  "+filin1(:iget-1))
 
    fils_out  = filin1(0:iget-1)
    do i = 0, iget - 1 
      fil_in     = filin1(i)
      fil_fix    = systemfunc("basename .grb2"+fil_in+" | cut -c 1-21")
      fix_yer    = ymdout(i)/10000
      fix_mon    = ymdout(i)/100 - fix_yer*100
      fil_out    = "${DIR1_NWP}/T639/"+sprinti("%4.4i",fix_yer)+"/"+sprinti("%2.2i",fix_mon)+"/"+fil_fix+".DAT"
;      print(fil_out)
;      exit

      ctim1  = systemfunc("basename "+fil_in+" | gawk -F. '{print \$3}' | cut -c  1-8 ")
      ctim2  = systemfunc("basename "+fil_in+" | gawk -F. '{print \$3}' | cut -c 9-13 ")
            
      fils_out(i) = sprinti("%8.8i", ymdout(i))+" "+sprinti("%2.2i",horout(i))+" "+ctim1+" "+ctim2+" "+fil_out
;      print((/fils_out(i)/))
      
      if ( isfilepresent( fil_out ) ) then
        if( "${MM_REEXT}" .eq. "FALSE") then
          continue
        else
          system("rm -rf "+fil_out)
        end if
      end if

      f_var    = addfile(fil_in, "r")
;      print(f_var)
      xlon     = f_var->lon_0
;      print(xlon)
      xlat     = f_var->lat_0
;      print(xlat)
      xplv     = f_var->lv_ISBL0
;    	print(xplv/100)

;; psfc    	
      dout  = f_var->PRES_P0_L1_GLL0(::-1, :)*0.01
      psfc  = dout
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; slp
      dout  = f_var->PRMSL_P0_L101_GLL0(::-1, :)*0.01
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; tsfc
      dout  = f_var->TMP_P0_L1_GLL0(::-1, :) 
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; satx
      dout  = f_var->TMP_P0_L103_GLL0(::-1, :) 
      t2m   = dout
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout )

;; uwnd
      dout  = f_var->UGRD_P0_L103_GLL0(::-1, :) 
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout )    	

;; vwnd
      dout  = f_var->VGRD_P0_L103_GLL0(::-1, :) 
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout ) 

;; saq
      RH    = f_var->RH_P0_L103_GLL0(::-1, :) 
      dout  = mixhum_ptrh(psfc, t2m, RH, -2)
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout ) 

;; ZSFC
      dout  = f_var->HGT_P0_L1_GLL0(::-1, :)*0.001
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout )
       
;; tlnd
      dout  = f_var->LAND_P0_L1_GLL0(::-1, :) 
;      printMinMax(dout, True)
      fbindirwrite( fil_out, dout )
      
;; tair
      do3d  = f_var->TMP_P0_L100_GLL0(:,::-1, :)            
;      printMinMax(do3d, True)
      fbindirwrite( fil_out, do3d )

;; h2ox
      do3d  = f_var->SPFH_P0_L100_GLL0(:,::-1, :)*1000. 
      do3d  = where(do3d .lt. 1.0e-4, 1.0e-4, do3d)           
;      printMinMax(do3d, True)
      fbindirwrite( fil_out, do3d )
 
    end do

;    if ( isfilepresent( "${fils_nwp}" ) ) then
;      system("rm -rf ${fils_nwp}" )
;    end if
;    print("${fils_nwp}")
;    asciiwrite ("${fils_nwp}", fils_out)

  end 
 
END_NCL
 
/home/f3cmon/ncl/bin/ncl ${cmd_fix}.ncl > .todelete
rm -rf ${cmd_fix}.ncl ${ftmp_nwp}
