#!/bin/bash

echo ""
echo "ENTER: SUB_DRXYL_BTOPB_SATXISTX_BIASSTDD_1CHN_BAQC.bash"

## To Define the SAT and Instriment.
DRSAT=${MM_SAT:-"FY3B"}
DRIST=${MM_IST:-"MWTS"}
DRREF=${MM_REF:-"NFNL"}
DRRTM=${MM_RTM:-"RTTOV101"}
DRFMT=${MM_FMT:-"NSMCHDF"}
echo ${DRSAT} ${DRIST} ${DRFMT} ${DRREF} ${DRRTM}
 
## Load the functions defined.
DRDIRROT=${MM_DIRROOT:-"/home/f3cmon/MONITOR"}
DRDIRRES=${MM_DIRDIRRES:-"./RESULTS"}
source ${DRDIRROT}/scripts/MyFunctions.bash
source ${DRDIRROT}/scripts/Environment_${DRSAT}.bash
 
eval ntmp=\$NCHN${DRIST}  ; let "NCHN=${ntmp:-54}"  ; printf "%s %4.4d\n" "NCHN: " ${NCHN}
eval ntmp=\$NPNT${DRIST}  ; let "NPNT=${ntmp:-15}"  ; printf "%s %4.4d\n" "NPNT: " ${NPNT}
eval ntmp=\$NADD${DRSAT}  ; let "NADD=${ntmp:-25}"  ; printf "%s %4.4d\n" "NADD: " ${NADD}
eval ntmp=\$NBIT${DRSAT}  ; let "NBIT=${ntmp:-4}"   ; printf "%s %4.4d\n" "NBIT: " ${NBIT}
let "NLEN = ${NADD} + ${NCHN} " 
echo ${DRSAT} ${DRIST} ${NCHN} ${NPNT} ${NADD} ${NLEN}

## Set the boundaries of valid data 
let "BL_YMDH=${MM_BYMDH:-2011010100}"
let "BU_YMDH=${MM_DYMDH:-2013123124}"
printf "%s %10.10d %10.10d\n" "Time Boundaries: " ${BL_YMDH} ${BU_YMDH}

## The day to display..
TIMIN1=$1
TIMIN2=$2
let "BG_YMD=${TIMIN1:-20131014}"
let "ED_YMD=${TIMIN2:-20131123}"
printf "%s %8.8d %8.8d\n" "Day to display: "  ${BG_YMD} ${ED_YMD}

#echo fils_in1= $1
#echo fils_in2= $2
#echo "FILS_IN1: $1 "

fils_in1="FY3B_MWTS_20131014_0000_to_20131123_1800_6H_RTTOV.HDF"
fils_in2="FY3B_MWTS_20131014_0000_to_20131123_1800_6H_CRTM.HDF"
echo "FILS_IN1: ${fils_in1} "

if [[ ! -e ${fils_in1} ]] ; then
  echo "File does not existe! exit"
exit 1
fi
 
cmd_fix="BTOPB_FY3BMWTS_BINX"

for (( ichn = 3; ichn <= 3; ichn++ )) ; do

  fil_out="TEST"
  echo ${fil_out}

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

    npnt=${NPNT}
    nlen=${NLEN}
    nchn=${NCHN}
 
;; get the number of days
    bg_ymd=${BG_YMD}
    ed_ymd=${ED_YMD}
    bg_yer=bg_ymd/10000
    ed_yer=ed_ymd/10000
    bg_mon=mod(bg_ymd/100, 100)
    ed_mon=mod(ed_ymd/100, 100)
;    print(bg_yer+"  "+ed_yer+" "+bg_mon+" "+ed_mon)
    ndys = (/31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31/)
    nday = 0
    do iyer = bg_yer, ed_yer
      if ( isleapyear(iyer) ) then
        ndys(1) = 29
      else
        ndys(1) = 28
      end if
 
      imn1  = 1
      imn2  = 12
      if ( iyer .eq. bg_yer ) then
        imn1  = bg_mon
      end if
      if ( iyer .eq. ed_yer ) then
        imn2  = ed_mon
      end if

      do imon = imn1, imn2
        nday  = nday + ndys(imon - 1) 
      end do
    end do
;    print (nday)
    
    ntim    = 4
    timlab  = new((/1, nday*ntim/), integer, -9999)
    irec    = 0
    do iyer = bg_yer, ed_yer
      if ( isleapyear(iyer) ) then
        ndys(1) = 29
      else
        ndys(1) = 28
      end if
      
      imn1  = 1
      imn2  = 12
      if ( iyer .eq. bg_yer ) then
        imn1  = bg_mon
      end if

      if ( iyer .eq. ed_yer ) then
        imn2  = ed_mon
      end if
            
      do imon = imn1, imn2
        do iday = 1, ndys(imon - 1)
          do ihor = 0, ntim - 1
            timlab(0, irec) = iyer*1000000 + imon*10000 + iday*100 + ihor*6
            irec =irec + 1
          end do
        end do
      end do
      
    end do  
  ;  print(timlab)
    ;exit

    ichn    = ${ichn} - 1

    fil_in1 = "${fils_in1}"
    f_var1  = addfile(fil_in1, "r")
 
    din11    = f_var1->Num
    din12    = f_var1->Avg
    din13    = f_var1->Stdp
    din14    = f_var1->Time
    timlab2 = din14(:, 0)
    timlab2 = din14(:, 0)*1000000 + din14(:, 1)*10000 + din14(:, 2)*100 + din14(:, 3)
;    printVarSummary(din1)
;    exit

    fil_in2 = "${fils_in2}"
    f_var2  = addfile(fil_in2, "r")
 
    din21    = f_var2->Num
    din22    = f_var2->Avg
    din23    = f_var2->Stdp
    din24    = f_var2->Time

    nnn     = dimsizes(timlab2)
    indxxx  = new(nnn, "integer", 0)
    do i = 0, nnn - 1
      indxxx(i)  = ind( timlab(0, :) .eq. timlab2(i) )
    end do
 
    numx            = new((/2, nday*ntim/), "float", 1.0e+35)
    bias            = numx
    stdd            = numx

    numx(0, indxxx)    = din11(:, ichn)/10000.    
    bias(0, indxxx)    = din12(:, ichn)
    stdd(0, indxxx)    = din13(:, ichn)

    numx(1, indxxx)    = din21(:, ichn)/10000.
    bias(1, indxxx)    = din22(:, ichn)
    stdd(1, indxxx)    = din23(:, ichn)

    ;num2           = numx 
    ;num2(0, :)  = numx(1, :)*10000.  - numx(0, :)*10000.
 
    xaix       = ispan(0, nday*ntim-1, 1) 

    if ( nday .gt. 600 ) then
      xttt       = ind(mod(timlab(0, :), 10000) .eq. 101 .or. mod(timlab(0, :), 10000) .eq. 401 .or. mod(timlab(0, :), 10000) .eq. 701 .or. mod(timlab(0, :), 10000) .eq. 1001 )      
      if ( mod( timlab(0, nday-1), 10000 ) .eq. 1231 .or. mod( timlab(0, nday-1), 10000 ) .eq. 331 .or. mod( timlab(0, nday-1), 10000 ) .eq. 630 .or. mod( timlab(0, nday-1), 10000 ) .eq. 930 ) then
        nnnn       = dimsizes(xttt)
        xind       = new(nnnn+1, "integer", -9999)
        xind(0:nnnn-1)  = xttt
        xind(nnnn) = nday-1 
      else       
        xind       = xttt
      end if
      caix       = sprinti("%4.4i",mod(timlab(0, xind), 10000) )+"~C~"+sprinti("%4.4i",timlab(0, xind)/10000 )
      xmain      = xaix(xind)      
      xmino      = ind(mod(timlab(0, :), 100) .eq. 1 .or. mod(timlab(0, :), 100) .eq. 15 )
    end if
 
    if ( nday .lt. 200 ) then
      xttt       = ind(mod(timlab(0, :), 10000) .eq. 100 .or. mod(timlab(0, :), 10000) .eq. 1500 )
    ;  print(xttt)
      nnnn       = dimsizes(xttt)
     ; print(nnnn)
      xind       = new(nnnn+1, "integer", -9999)
      xind(0:nnnn-1)  = xttt
    ;  print(xind)
      xind(nnnn) = nday*ntim-1
      delete(xttt)
      caix       = sprinti("%4.4i",mod(timlab(0, xind), 1000000)/100 )+"~C~"+sprinti("%4.4i", timlab(0, xind)/1000000  )
    ;  print(caix)
      xttt       = ind( mod(timlab(0, :), 200) .eq. 0 )
      xmain      = xaix(xind)
    ;  print(xmain)
      xmino      = xaix(xttt)
    end if
 
;; plots
    _Font      = 4 
    ;;wks = gsn_open_wks("png", "${fil_out}" )
    wks = gsn_open_wks("png", "${fil_out}" )
    gsn_define_colormap(wks,"default")
    alphas             = (/"a)","b)","c)","d)","e) ","f) ","g) ","h) ","i) ", "j) ", "k) "/)     
    xyLTs              = (/.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05,.05/)*40.  
    xyLCs              = (/27,2, 3, 4,8,7,6,10,12,13,16,19,20,22,23,24,28/)
    xyDPs              = (/0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0/)
    plot               = new(4,graphic)

    do ilp1 = 0 , 0

      res                                = True

      res@gsnMaximize                    = False               ; maximize pot in frame
      res@gsnFrame                       = False               ; don''t advance frame
      res@gsnDraw                        = False               ; don''t draw plot
      res@gsnPaperOrientation            = "portrait" 

      res@vpXF                           = 0.10
      res@vpHeightF                      = 0.15
      res@vpWidthF                       = 0.85

      res@xyMarkLineMode                 = "Lines"  ; "Markers", "MarkLines"
      res@xyLineThicknesses              = xyLTs    ; thicker line
      res@xyDashPatterns                 = xyDPs
      res@xyLineColors                   = xyLCs(:10)
      res@xyMarker                       = 16    ; 3 different markers
      res@xyMarkerColors                 = xyLCs(:10)
      res@xyMarkerSizeF                  = 0.005

      res@trXMinF                        = 0 
      res@trXMaxF                        = nday*ntim - 1
 
;;   For X-Top
      res@trXReverse                     = False        
      res@tmXTBorderOn                   = True
      res@tmXTOn                         = True
      res@tmXTLabelsOn                   = False
      
;;   For X-Bottom                         
      res@tmXBBorderOn                   = True
      res@tmXBOn                         = True
      res@tmXBLabelsOn                   = False
      res@tmXBLabelFont                  = _Font
      res@tmXBLabelFontHeightF           = 0.015
      res@tmXBLabelAngleF                = 0
      res@tmXBLabelFontThicknessF        = 2.0
      res@tmXBMode                       = "Explicit"
      res@tmXBValues                     = xmain
      res@tmXBLabels                     = caix
      res@tmXBMinorOn                    = True
      res@tmXBMinorValues                = xmino

;      res@trYMinF                        = 0 
;      res@trYMaxF                        = 3.0

;;  For Y-Right
      res@trYReverse                     = False
      res@tmYRBorderOn                   = True
      res@tmYROn                         = True
      res@tmYRLabelsOn                   = False
      
;;  For Y-Left      
      res@tmYLBorderOn                   = True
      res@tmYLOn                         = True
      res@tmYLLabelsOn                   = True
      res@tmYLLabelFont                  = _Font
      res@tmYLLabelFontHeightF           = 0.015
      res@tmYLLabelAngleF                = 0
      res@tmYLLabelFontThicknessF        = 2.0      
;      res@tmYLMode                       = "Explicit"
;      res@tmYLValues                     = ispan(160, 320, 20)
;      res@tmYLLabels                     = sprinti("%0.3i", ispan(160, 320, 20))       
;      res@tmYLMinorValues                = ispan(160, 320, 5)
;      res@tmYLMinorOn                    = True;


      res@gsnStringFont                  = _Font
      res@gsnStringFontHeightF           = 0.015           
;      res@gsnLeftStringOrthogonalPosF    = 0.03   

;; for tittles
      res@tiMainFont                     = _Font
      res@tiMainFontHeightF              = 0.012
      res@tiMainFontThicknessF           = 2.0
      res@tiXAxisString                  = ""
      res@tiXAxisFont                    = _Font
      res@tiXAxisFontHeightF             = 0.015
      res@tiXAxisFontThicknessF          = 2.0
      res@tiXAxisOffsetYF                = 0.00     ;; positive to up
      res@tiXAxisOffsetXF                = 0.0       ;; positive to right
      
      res@tiYAxisString                  = ""
      res@tiYAxisFont                    = _Font
      res@tiYAxisFontHeightF             = 0.016
      res@tiYAxisFontThicknessF          = 2.0      
      res@tiYAxisOffsetYF                = 0.0        ;; positive to up
      res@tiYAxisOffsetXF                = -0.02      ;; positive to right

      res@trYMinF                        = 0.
      res@trYMaxF                        = 10.
      res@gsnLeftString                  = "a) NUM"
      res@gsnCenterString                = "" 
      res@gsnRightString                 = "10~S~4~N~   " 
      res@vpYF                           = 0.95
      plot(0)  = gsn_csm_xy(wks, xaix, numx , res)

      res@trYMinF                        = -1.5
      res@trYMaxF                        = -0.5
      res@gsnLeftString                  = "b) BIAS"
      res@gsnCenterString                = "" 
      res@gsnRightString                 = " K  "
      res@vpYF                           = 0.73
      plot(1)  = gsn_csm_xy(wks, xaix, bias, res)  


      res@trYMinF                        = 0.2
      res@trYMaxF                        = 0.7
      res@gsnLeftString                  = "c) STDD"
      res@gsnCenterString                = "" 
      res@gsnRightString                 = " K  "
      res@tmXBLabelsOn                   = False
      res@vpYF                           = 0.50
      res@tmXBLabelsOn                   = True
      plot(2)  = gsn_csm_xy(wks, xaix, stdd, res)         
  
    end do  
    
      LS   = (/"RTTOV","CRTM","CHN 3","CHN 4","Min Chan" /)
      gsres                    = True
      gsres@gsMarkerIndex      = 16
      gsres@gsMarkerThicknessF = 2.0

      lint = 0.3
      ladd = -0.03
      do i = 0, 1
        respl                    = True
        respl@gsLineThicknessF   = 2.0
        respl@gsLineColor        = xyLCs(i)
        respl@gsLineThicknessF   = xyLTs(i)*2.
        respl@gsLineDashPattern  = xyDPs(i)

        gsn_polyline_ndc(wks, (/-0.04,0.04/)+lint*(i+1)+ladd, (/0.25, 0.25/), respl)
        gsres@gsMarkerColor      = xyLCs(i)

        restx                    = True
        restx@txFontHeightF      = 0.016
        restx@txFont             = _Font
        restx@txJust             = "CenterLeft"

        gsn_text_ndc (wks, LS(i), lint*(i+1) + 0.05+ladd ,0.25 ,restx)
      end do     
 
    draw(plot)
    frame(wks)     
  end 
END_NCL

/home/f3cmon/ncl/bin/ncl ${cmd_fix}.ncl
#rm -rf  ${cmd_fix}.ncl

done



