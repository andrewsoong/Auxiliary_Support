;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;------------------------------------------------------------------------


undef("ASGNbin_VALUE_opt")
function ASGNbin_VALUE_opt( datinx[*]:numeric, resin:logical )

begin       

  if ( .not. resin .or. .not. isatt(resin,"nbin") )  then 
    print("  Not All obligatory resource is setted. exit1")
    iout   = 1
    return(iout)
  end if

  if ( resin .and. isatt(resin,"nbin2add") )  then 
    nbin2a = resin@nbin2add
  else
    nbin2a = 0
  end if  

  if ( resin .and. isatt(resin,"nbin2minus") )  then 
    nbin2m = resin@nbin2minus
  else
    nbin2m = 0
  end if 
  nnbin    = nbin2a + nbin2m + 1
  nbinmax  = resin@nbin + nbin2a
  print(resin@nbin+"  "+nnbin+"  "+nbinmax)
  binmax   = new((/nnbin/), "float", 1.0e+35)

  datin    = datinx
;;;;
  avg1      = dim_avg(datin)
  min1      = min(datin)
  max1      = max(datin)
  dif1      = max1 - min1
  print("stage1: "+avg1+" "+min1+" "+max1+" "+dif1)
  
  npdfx    = 200
  if ( resin .and. isatt(resin,"DataRetain") )  then 
    DataRetain = resin@DataRetain
  else
    DataRetain = 99.99
  end if
  avg0      = dim_avg(datin)
  zopt      = True
  zpdf      = pdfx( abs(datin - avg0), npdfx, zopt)
  do i = 1, npdfx - 1
    if ( sum(zpdf(npdfx-1-i:npdfx-1)) .gt. 100. - DataRetain ) then
      jbin  = npdfx - i
      break
    end if
  end do
  datin     = where( abs(datin - avg0) .gt. zpdf&x(jbin), datin@_FillValue, datin ) 

;;;;
  avg1      = dim_avg(datin)
  min1      = min(datin)
  max1      = max(datin)
  dif1      = max1 - min1
  print("stage2: "+avg1+" "+min1+" "+max1+" "+dif1)
  iplc      = get_coef2int_lt_10(dif1)
  coef      = 10^iplc  

 
  if ( min1 .lt. 0 ) then
    min2  = toint( min1 / coef - 1 )
  else
    min2  = toint( min1 / coef )
  end if
  if ( max1 .lt. 0 ) then
    max2  = toint( max1 / coef )
  else
    max2  = toint( max1 / coef + 1 )
  end if
  dif2    = max2 - min2
  print("stage3: "+avg1+" "+min2+" "+max2+" "+dif2) 
  do iii = 0, nnbin - 1
    nnn  = resin@nbin - nbin2m + iii
    intv = (max2 - min2 )*1.0/nnn 
    intv = toint(intv*10. + 0.999 ) / 10.0
    binmax(iii)  = min2 + nnn*intv
;    print(iii+"  "+nnn+"  "+intv)
  end do
  iii    = minind( binmax - max2 )
  nbin   = resin@nbin - nbin2m + iii + 1
  binx   = fspan(min2, binmax(iii), nbin)
  min3   = min2
  max3   = binmax(iii)
 
  iout   = 0
  iout@bin   = binx
  iout@nbin  = nbin
  iout@max   = max3
  iout@min   = min3
  iout@iplc  = iplc
  return(iout)
 
end
;------------------------------------------------------------------------
; MAIN CODE
;------------------------------------------------------------------------

begin    
    fil_in = file_in
    f_var1 = addfile(fil_in, "r")      
    datin = f_var1->agc1_3day   
    dtime = f_var1->time_3day  
   dtimlab = dtime(:, 0)
   dtimlab = dtime(:, 0)*1000000 + dtime(:, 1)*10000 + dtime(:, 2)* 100+ dtime(:,3)   
   
   dlth = dimsizes(dtimlab) 
   dx = ispan(0, dlth-1, 1) 
   
   dy = new((/1,dlth/), "float", 1.0e+35)    
   dy(0, dx) = datin     
 
  dlthstep = (dlth-1)/5
  dxs = ispan(0, dlth-1, dlthstep)  
  dxlbs = sprinti("%4.4i",mod(dtimlab(dxs), 10000) )+"~C~"+sprinti("%4.4i", mod(dtimlab(dxs)/10000,10000)) 
 ;  dxlbs = sprinti("%4.4i",mod(dtimlab(dxs), 10000) )+"~C~"+sprinti("%4.4i", dtimlab(dxs)/10000) 
  
      
     nbin          = 180
  ncolors       = nbin+1
  ncolors1      = 11
;;;; Get the Valid data range.
  if ( resin .and. isatt(resin,"ValRange1") .and. isatt(resin,"ValRange2") ) then
    Rmin1    = resin@ValRange1
    Rmax1    = resin@ValRange2  
    datin    = where( datin .gt. Rmax1 .or. datin .lt. Rmin1, datin@_FillValue, datin )

  else
 
    if ( resin .and. isatt(resin,"DataRetain") )  then 
      DataRetain = resin@DataRetain
    else
      DataRetain = 99.
    end if
    resbin       = True
    resbin@nbin  = ncolors1
    resbin@nbin2add   = 1
    resbin@nbin2minus = 1
    resbin@DataRetain = DataRetain
;    printMinMax(datin, True)
    iout = ASGNbin_VALUE_opt( datin, resbin )
   
   
   
   
   
   
;; plots
    _Font      = 4   
    wks = gsn_open_wks("png", file_out)
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
      res@txFontThicknessF               = 2   
      res@vpXF                           = 0.1
      res@vpHeightF                      = 0.12
      res@vpWidthF                       = 0.85
      
      res@xyMarkLineMode                 = "Lines"  ; "Markers", "MarkLines"
      res@xyLineThicknesses              = xyLTs    ; thicker line
      res@xyDashPatterns                 = xyDPs
      res@xyLineColors                   = xyLCs(:10)
      res@xyMarker                       = 16    ; 3 different markers
      res@xyMarkerColors                 = xyLCs(:10)
      res@xyMarkerSizeF                  = 0.005     

;;   For X-Top
      res@trXReverse                     = False        
      res@tmXTBorderOn                   = True
      res@tmXTOn                         = True
      res@tmXTLabelsOn                   = False
      

    ; res@tmXBValues                     = dx
   ;   res@tmXBLabels                     = caix
    ;  res@tmXBMinorOn                    = True
   ;   res@tmXBMinorValues                = xmino


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
      res@tmYLLabelFontHeightF           = 0.012
      res@tmYLLabelAngleF                = 0
      res@tmYLLabelFontThicknessF        = 2.0  
      

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
     ; res@tiYAxisFontHeightF             = 0.012
      res@tiYAxisFontThicknessF          = 2.0      
      res@tiYAxisOffsetYF                = 0.0        ;; positive to up
      res@tiYAxisOffsetXF                = -0.02      ;; positive to right       
      
      res@gsnStringFont                  = _Font
      res@gsnStringFontHeightF           = 0.012           
      res@gsnLeftStringOrthogonalPosF    = 0.02  
      res@gsnCenterString                = "" 
      
      
  ;;   For X-Bottom                         
      res@tmXBBorderOn                   = True
      res@tmXBOn                         = True
      res@tmXBLabelsOn                   = True
      res@tmXBLabelFont                  = _Font
      res@tmXBLabelFontHeightF           = 0.01
      res@tmXBLabelAngleF                = 0
      res@tmXBLabelFontThicknessF        = 2.0
     
     res@tmXBMode                       = "Explicit"
     res@tmXBValues                     = dxs
     res@tmXBLabels                     = dxlbs
    ; res@tmXBMinorOn                    = True
     ;res@tmXBMinorValues                = dxmino
     res@gsnLeftString                  = sub_title(0)  
     res@trYMinF                        = min3
     res@trYMaxF                        = max3 
     res@vpYF                           = 0.9     
     plot(0)  = gsn_csm_xy(wks, dx , dy , res)    
   
      restx                    = True
      restx@txFontHeightF      = 0.016
      restx@txFont             = _Font
      restx@txFontThicknessF   = 2
      restx@txJust             = "CenterLeft"
      gsn_text_ndc (wks, file_title, 0.13,0.98 ,restx)   
    end do 
     
    draw(plot)
    frame(wks)     
end
   print(">> Failed to create "+file_out+".png ! >>")


