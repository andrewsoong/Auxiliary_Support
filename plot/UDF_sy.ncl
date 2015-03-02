;;;;----------------------------------------------------------------------------------------------------------
;;;;                PART ONE: FUNCTOIN/PROCEDURE FOR CALCULATING. 
;;;;----------------------------------------------------------------------------------------------------------
;;
;; function Gen_timlab(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_dkd(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_mon(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_bmn(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_ssn(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_hfy(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_yer(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_evy(timbeg:numeric, timend:numeric )
;; function ASGNtm_daylab_opt(timbeg:numeric, timend:numeric, draw_typ:string )
;;
;;;;----------------------------------------------------------------------------------------------------------
;;;;                PART TWO: FUNCTOIN/PROCEDURE FOR PLOT DRAWING
;;;;----------------------------------------------------------------------------------------------------------
;; 
;; function udf_contour_timspt( wks:graphic, datin[*][*]:numeric, timlab[*]:numeric, scanpnt[*]:numeric, resin:logical )
;; 
;; 
;; 

;;;;----------------------------------------------------------------------------------------------------------
;;;;                PART ONE: FUNCTOIN/PROCEDURE FOR CALCULATING. 
;;;;----------------------------------------------------------------------------------------------------------

function get_coef2int_lt_10( datin:numeric )
begin
  datmp   = datin
  coef    = 0
  if ( datmp .lt. 1 ) then
    do i = 1, 10
      datmp = datmp * 10.
      coef  = coef - 1
      if ( datmp .ge. 1 ) then
        break
      end if
    end do
  else
    if ( datmp .gt. 10 ) then
      do j = 1, 10
        datmp = datmp / 10.
        coef  = coef + 1
        if ( datmp .le. 10 ) then
          break
        end if
      end do
    end if
  end if
  return(coef)
end


;;;;
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
 ; print("stage1: "+avg1+" "+min1+" "+max1+" "+dif1)
  
  npdfx    = 200
  datin@_FillValue =1.0e+35
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
 ; print("stage2: "+avg1+" "+min1+" "+max1+" "+dif1)
  iplc      = get_coef2int_lt_10(dif1)
  coef      = 10^iplc  
;  print(iplc+"  "+coef)
;  min2      = min1 / coef
;  max2      = max1 / coef
;  dif2      = max2 - min2
 
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
 ; print("stage3: "+avg1+" "+min2+" "+max2+" "+dif2) 
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
;;;;----------------------------------------------------------------------------------------------------------
;;;;                PART TWO: FUNCTOIN/PROCEDURE FOR PLOT DRAWING
;;;;----------------------------------------------------------------------------------------------------------


function histo4_1d( wks:graphic, datinx[*]:numeric, latin[*]:numeric, \
                    lonin[*]:numeric, resin:logical )

begin
  if( .not. resin .or. .not. isatt(resin,"iplot") .or. .not. isatt(resin,"nplot") ) then
    print("The resource var (resin) has not been setted correctly, PLEASE CHECKE! EXIT!")
    exit
  end if

  datin   = datinx
 
;; Get the distribution of plots
;;;; nxxx: number of columns of the paneled lots
;;;; nyyy: number of rows of the paneled lots
;;;;;; determined by the resource setted or calculated inner.
  if resin .and. isatt(resin, "nxxx" ) .and. isatt(resin, "nyyy" ) then
    nxxx      = resin@nxxx
    nyyy      = resin@nyyy
  else
    if ( resin@nplot .le. 2 ) then
      if ( resin .and. isatt(resin, "gsnPaperOrientation") .and. resin@gsnPaperOrientation .eq. "LandScape" ) then
        nxxx    = resin@nplot
        nyyy    = 1
      else
        nxxx    = 1
        nyyy    = resin@nplot
      end if
    else
      if ( resin@nplot .le. 6 ) then
        nxxx  = 2
        nyyy  = (resin@nplot + 1) / 2
      else
        nxxx  = 3
        nyyy  = 3
      end if
    end if
  end if
;  print("NXX: "+nxxx+" NYY: "+nyyy)

;;;; ixxx: column index of the paneled lots
;;;; iyyy: row index of the paneled lots
  if resin .and. isatt(resin, "nxxx" ) .and. isatt(resin, "nyyy" ) then  
    ixxx      = resin@ixxx
    iyyy      = resin@iyyy
  else
    ixxx      = mod(resin@iplot, nxxx)
    iyyy      = resin@iplot / nxxx
  end if

  if ( ixxx .gt. nxxx .or. iyyy .gt. nyyy ) then
    print("ERROR IN SETTING number/index of COLUMNS/ROWS PLOTTED, exit")
    iout      = 1
    exit
  end if

;;;; Get the Size of plot
;;;;;; The reference Size when one PLOT only. 
  BaseX     = 0.97
  BaseY     = 0.82

;;;;;; Get the Ratio of sub-plot to BaseX/BaseY
  if ( nxxx .ge. nyyy ) then
    mprwdt  = 1.000 / nxxx 
    mprhgt  = mprwdt
  else
    mprhgt  = 1.000 / nyyy / BaseY
    mprwdt  = mprhgt 
  end if
;;;;;; The distance between the left/top edges of two sub-plots 
  dltx      = BaseX * mprwdt+0.1
  dlty      = BaseY * mprhgt
;;;;;; Position (Top-Left corner) of the map in sub-plot  
  mapvpx    = 0.000 + 0.08*sqrt(mprwdt) + ixxx*dltx
  mapvpy    = 0.990 - 0.05*sqrt(mprhgt) - iyyy*dlty

;;;;;; The Font and Height of strings
  _Font     = 4 
  _FontHGT  = 0.013 / sqrt( sqrt(1.0/mprwdt) ) 

;;;;;; Position (Top-Left corner) of the histogram in sub-plot
  hisvpy    = mapvpy - 0.45 * mprhgt*1.1 - _FontHGT*2.0
  hishgt    = 0.05 * mprhgt * 1.2

;;;;;; Position (Top-Left corner) of the Color bar in sub-plot  
  lb1vpy    = hisvpy - hishgt - 0.01*mprhgt
  lb1hgt    = 0.03 * mprhgt
;;;;;; Position (Top-Left corner) of the Label of Color bar in sub-plot
  lb2vpy    = hisvpy - hishgt - 0.005*mprhgt
  lb2hgt    = 0.03 * mprhgt


  LeftStrs  = (/"a) ","b) ","c) ","d) ","e) ", "f) ", "g) ", "h) ","i) ", "j) "/)

;;;; for Latitudes/Longitudes 
  if ( resin .and. isatt(resin,"mpCenterLonF") ) then
    mpCenterLonF      = ( toint(resin@mpCenterLonF) + 29 ) / 30 * 30 
  end if
  if( resin .and. isatt(resin,"mpCenterLonF") )  then  
    mpMinLonReF       = -180. + mpCenterLonF
    mpMaxLonReF       =  180. + mpCenterLonF
  end if
  mpMinLatF           =  -90.
  mpMaxLatF           =   90.
  mpMinLonF           =    0.
  mpMaxLonF           =  360.
  if ( resin .and. isatt(resin,"mpMinLatF") ) then 
    mpMinLatF         = resin@mpMinLatF
  end if
  if ( resin .and. isatt(resin,"mpMaxLatF") ) then 
    mpMaxLatF         = resin@mpMaxLatF
  end if
  if ( resin .and. isatt(resin,"mpMinLonF") ) then 
    mpMinLonF         = resin@mpMinLonF
  end if
  if ( resin .and. isatt(resin,"mpMaxLonF") ) then 
    mpMaxLonF         = resin@mpMaxLonF
  end if 
 
  if ( mpMinLonF .lt. mpMinLonReF .or. mpMaxLonF .gt. mpMaxLonReF ) then
    if ( ( mpMaxLonF - mpMinLonF ) .lt. 300 ) then
      print("Setting mpMax/MinLon is inconsistent with that of mpCenterLon, pleast CHECK!")
      print(mpMinLonF+" "+mpMinLonF+" "+resin@mpCenterLonF)
      exit
    end if
  end if
  if ( ( mpMaxLonF - mpMinLonF ) .gt. 300 ) then
      mpMinLonF           = mpMinLonReF
      mpMaxLonF           = mpMaxLonReF
  end if

;;;; for the TickMark of YL
  if ( resin .and. isatt(resin,"tmYLLabels") .and. isatt(resin, "tmYLValues") ) then
    LatVALs = resin@tmYLValues
    LatLABs = resin@tmYLLabels
  else     
    if ( mprhgt .gt. 0.4 ) then
      LatVALs           = (/-3,-2, -1, 0, 1, 2, 3/)*30
      LatLABs           = (/"90S","60S","30S","Eq", "30N", "60N", "90N"/)      
    else
      LatVALs           = (/-3, -1.5, 0, 1.5, 3/)*30
      LatLABs           = (/"90S", "45S", "Eq", "45N", "90N"/)       
    end if
  end if
;;;; for the TickMark of XB  
  if ( resin .and. isatt(resin,"tmXBLabels") .and. isatt(resin, "tmXBValues") ) then
    LonVALs = resin@tmXBValues
    LonLABs = resin@tmXBLabels
  else   
    if ( mprwdt .gt. 0.5 ) then
      LonVALs           = ispan(toint(mpMinLonReF), toint(mpMaxLonReF), 30)*1.0
;      LonLABs           = (/"0E","30E","60E","90E","120E","150E","180","150W","120W","90W", "60W","30W","0W"/)  
    else
      LonVALs           = ispan( toint(mpMinLonReF), toint(mpMaxLonReF), 60 )*1.0
;      LonLABs           = (/"0E","60E","120E","180","120W","60W","0W"/)
    end if
    ndim     = dimsizes(LonVALs)
    LonLABs = new(ndim, "string")
    do idim = 0, ndim - 1
      if ( LonVALs(idim) .lt. 0 .or. LonVALs(idim) .gt. 180 ) then
        if ( LonVALs(idim) .lt. 0 ) then
          LonLABs(idim)  = sprinti("%i", abs(toint(LonVALs(idim))) )+"W"
        else
          LonLABs(idim)  = sprinti("%i", abs(360 - toint(LonVALs(idim))) )+"W"
        end if
      else
        LonLABs(idim)  = sprinti("%i", abs(toint(LonVALs(idim))) )+"E"
      end if
      if ( LonVALs(idim) .eq. 0 .or. LonVALs(idim) .eq. 360 ) then
        if ( idim .ne. 0 .and. idim .ne. ndim - 1 ) then
          LonLABs(idim)  = "0"
        else
          if ( idim .eq. 0 ) then
            LonLABs(idim)  = "0E"
          else
            LonLABs(idim)  = "0W"
          end if
        end if
      end if
      if ( LonVALs(idim) .eq. 180 ) then 
        if ( idim .ne. 0 .and. idim .ne. ndim - 1 ) then
          LonLABs(idim)  = "180"
        else
          if ( idim .eq. 0 ) then
            LonLABs(idim)  = "180W"
          else
            LonLABs(idim)  = "180E"
          end if
        end if         
      end if
    end do 
  end if
 
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
    ;print( iout@bin  )
    ;print( iout@nbin )
    ;print( iout@max  )
    ;print( iout@min  )
    ;print( iout@iplc ) 
    ;exit
    ncolors1  = iout@nbin
;    printMinMax(datin, True)
;    exit
 
    iplc      = iout@iplc 
    coef      = 10^iplc
    Rmin1     = iout@min * coef
    Rmax1     = iout@max * coef
    datin@_FillValue =1.0e+35
    datin     = where( datin .gt. Rmax1 .or. datin .lt. Rmin1, datin@_FillValue, datin )
  end if

  n1             = minind(datin)
  if ( all( ismissing(n1) ) ) then
    datin(0)     = Rmin1
  else 
    datin(n1(0)) = Rmin1
  end if
  n2             = maxind(datin)
  if ( all( ismissing(n2) ) ) then
    datin(1)     = Rmax1
  else 
    datin(n2(0)) = Rmax1
  end if  
  delete(n1)
  delete(n2)
  Rmean          = dim_avg_n(datin, 0)  ; 0.621 
  Zero1          = datin
  Zero1          = 0.0
  Rrmse          = dim_rmsd_n(datin, Zero1, 0)
;  print("Hello: "+Rrmse+" : "+Rmean)
  delete(Zero1)
 
;;; for the histogram
  nbin     = 180
  Rstep    = (Rmax1 - Rmin1)/nbin
  arr      = new(nbin, float) 
  colors   = new(nbin+1, integer)
  do i = 0, nbin-1
   arr(i)    = Rmin1 + i*Rstep
   colors(i) = i+3
  end do
  colors(nbin)=nbin+3
 
;------------------------------ 
; Create X and Y arrays to hold the points for each range and initialize
; them to missing values.  We want to use num_distinct_markers
; different colors, so we need num_distinct_markers sets of X and
; Y points.
  num_distinct_markers = dimsizes(arr) + 1        ; number of distinct markers
  lat_new = new((/num_distinct_markers, dimsizes(datin)/), float, -999)
  lon_new = new((/num_distinct_markers, dimsizes(datin)/), float, -999)
;
; Group the points according to which range they fall in. At the
; same time, create the label that we will use later in the legend.
;       
  do i = 0, num_distinct_markers-1
    if (i.eq.0) then
      indexes = ind(datin .lt. arr(0))
      ;labels(i) = "x < " + arr(0)
    end if
    if (i.eq.num_distinct_markers-1) then
      indexes = ind(datin.ge.max(arr))
      ;labels(i) = "x >= " + max(arr)
    end if
    if (i.gt.0.and.i.lt.num_distinct_markers-1) then       
      indexes = ind(datin.ge.arr(i-1).and.datin.lt.arr(i))
      ;labels(i) = arr(i-1) + " <= x < " + arr(i)
    end if
;
; Now that we have the set of indexes whose values fall within 
; the given range, take the corresponding lat/lon values and store
; them, so later we can color this set of markers with the appropriate
; color.
;
    if (.not.any(ismissing(indexes))) then 
      npts_range = dimsizes(indexes)   ; # of points in this range.
      lat_new(i,0:npts_range-1) = latin(indexes)
      lon_new(i,0:npts_range-1) = lonin(indexes)
    end if
    delete(indexes) 
    if (( Rmean .ge. Rmin1+i*Rstep ) .and. ( Rmean .lt. Rmin1+(i+1)*Rstep ) )
      Mbin = i
    end if
  end do
  
  gsn_define_colormap(wks,"BlAqGrYeOrReVi200")       ; choose colormap
  nc1 = NhlNewColor(wks,.8,.8,.8)        ; Add light gray to colormap,

;;;; The Geo-distribution plot 
  mpres                       = True
  mpres@gsnMaximize           = False           ; Maximize plot in frame.
  mpres@gsnFrame              = False           ; Don't advance the frame
 
print(mapvpx)
print(mapvpy)

  mpres@vpXF                  = mapvpx
  mpres@vpYF                  = mapvpy 
  mpres@vpWidthF              = 0.90 * mprwdt+0.15
  mpres@vpHeightF             = 0.45 * mprhgt
 print(mpres@vpWidthF)
print(mpres@vpHeightF)
  mpres@mpMinLatF             = mpMinLatF
  mpres@mpMaxLatF             = mpMaxLatF
  mpres@mpMinLonF             = mpMinLonF
  mpres@mpMaxLonF             = mpMaxLonF
  mpres@tmXBMode              = "Explicit"
  mpres@tmXBMinorOn           = True
  mpres@tmXBMinorValues       = ispan(-360, 540,10)*1.0
  mpres@tmXBValues            = LonVALs
  mpres@tmXBLabels            = LonLABs
  mpres@tmYLMode              = "Explicit"
  mpres@tmYLMinorValues       = ispan(-90,90,5) 
  mpres@tmYLMinorOn           = True 
  mpres@tmYLValues            = LatVALs
  mpres@tmYLLabels            = LatLABs
  mpres@mpCenterLonF          = mpCenterLonF
 
  mpres@mpFillColors          = (/-1,-1,nc1,-1/)    ;assign light gray to land masses
  if ( resin .and. isatt(resin,"tmXBLabelFont") ) then
    mpres@tmXBLabelFont       = resin@tmXBLabelFont
  else
    mpres@tmXBLabelFont       = _Font
  end if
  if ( resin .and. isatt(resin,"tmXBLabelFontHeightF") ) then
    mpres@tmXBLabelFontHeightF  = resin@tmXBLabelFontHeightF
  else
    mpres@tmXBLabelFontHeightF  = _FontHGT
  end if
  mpres@tmYROn                = True                    ; no YR tick marks
  mpres@tmXTOn                = True                    ; no XT tick marks
  mpres@tmYLOn                = True                    ; no YL tick marks
  mpres@tmXBOn                = True                    ; no XB tick marks
 
  mpres@tmYLLabelsOn        = True
  mpres@tmXBLabelsOn          = True
  if ( resin .and. isatt(resin,"tmYLLabelFont") ) then
    mpres@tmYLLabelFont       = resin@tmYLLabelFont
  else
    mpres@tmYLLabelFont       = _Font
  end if
  if ( resin .and. isatt(resin,"tmYLLabelFontHeightF") ) then
    mpres@tmYLLabelFontHeightF  = resin@tmYLLabelFontHeightF
  else
    mpres@tmYLLabelFontHeightF  = _FontHGT
  end if
  if ( resin .and. isatt(resin,"gsnStringFont") ) then
    mpres@gsnStringFont       = resin@gsnStringFont
  else
    mpres@gsnStringFont       = _Font
  end if
  if ( resin .and. isatt(resin,"gsnStringFontHeightF") ) then
    mpres@gsnStringFontHeightF  = resin@gsnStringFontHeightF
  else
    mpres@gsnStringFontHeightF  = _FontHGT
  end if
  if ( resin .and. isatt(resin,"gsnLeftString") ) then 
    mpres@gsnLeftString       = resin@gsnLeftString
  else
    mpres@gsnLeftString       = LeftStrs( resin@iplot )
  end if
  iplc1    = get_coef2int_lt_10(Rmean)
  if ( abs( iplc1 ) .gt. 2 ) then
    strmean    = sprintf("%4.2E", Rmean )
  else
    if ( iplc1 .gt. 0 )
      iii1     = 2 + abs(iplc1)
      iii2     = 1 + abs(iplc1)
    else
      iii1     = 2 + abs(iplc1)  
      iii2     = 2 
    end if
    strmean    = sprintf("%"+iii1+"."+iii2+"f", Rmean )
  end if
    
  iplc1    = get_coef2int_lt_10(Rrmse)
  if ( abs( iplc1 ) .gt. 2 ) then
    strrmse    = sprintf("%4.2E", Rrmse )
  else
    if ( iplc1 .gt. 0 )
      iii1     = 2 + abs(iplc1)
      iii2     = 1 + abs(iplc1)
    else
      iii1     = 2 + abs(iplc1)  
      iii2     = 2 
    end if
    strrmse    = sprintf("%"+iii1+"."+iii2+"f", Rrmse )
  end if
  if ( resin .and. isatt(resin,"gsnRightString") ) then 
    mpres@gsnRightString       = resin@gsnRightString
  else
    mpres@gsnRightString       = "MEAN: "+strmean+", RMSE: "+strrmse+" "
  end if
 
  map = gsn_csm_map(wks,mpres)

;;;; label bar - colors 
  lbres                               = True
  lbres@vpXF                          = mapvpx
  lbres@vpYF                          = lb1vpy
  lbres@vpWidthF                      = 0.9 * mprwdt    ; Width of labelbar
  lbres@vpHeightF                     = lb1hgt          ; Height of labelbar
  lbres@lbAutoManage                  = True
  lbres@lbOrientation                 = "Horizontal"
  lbres@lbMonoFillPattern             = True
  lbres@lbFillColors                  = colors          ; ispan(0,ncolors-1,1)                                  
  lbres@lbPerimOn                     = False           ; Turn off box around labelbar.   

  lbres@lbBottomMarginF               = 0.0
  lbres@lbTopMarginF                  = 0.0
  lbres@lbLeftMarginF                 = 0.0
  lbres@lbRightMarginF                = 0.0
  lbres@lbBoxMajorExtentF             = 1.0
  lbres@lbBoxMinorExtentF             = 1.0
  
  labels = new (ncolors, string)
  labels = " " 
  lbres@lbBoxLinesOn                  =  False

  gsn_labelbar_ndc(wks, ncolors, labels, lbres@vpXF, lbres@vpYF, lbres)  ; Draw a labelbar
;  print(lbres@vpXF+"  "+lbres@vpYF+" "+lbres@vpWidthF+" "+lbres@vpHeightF)

;;;; label bar - box 
  lres                                = True
  lres@vpXF                           = mapvpx
  lres@vpYF                           = lb2vpy
  lres@vpWidthF                       = 0.9 * mprwdt    ; Width of labelbar
  lres@vpHeightF                      = lb2hgt      ; Height of labelbar
 
  lres@xyLineColors                   = (/"black"/)          ; change line color
  
  lres@tmXBMode                       = "Explicit"
  lres@tmYROn                         = False                    ; no YR tick marks
  lres@tmXTOn                         = False                    ; no XT tick marks
  lres@tmYLOn                         = False                    ; no YL tick marks
  lres@tmXBOn                         = False                    ; no XB tick marks
  lres@tiYAxisOn                      = False
  lres@trXMinF                        = 0 ;xmin
  lres@trXMaxF                        = 20 ;xmax
  lres@trYMinF                        = 0 ; ymin
  lres@trYMaxF                        = 0.2 ;ymax
  lres@tmXBTickSpacingF               = 2
  lres@tmXBValues                     = (/0,2,4,6,8,10,12,14,16,18,20/)
  
  bval = fspan(Rmin1, Rmax1, ncolors1)
  lres@tmXBLabels                     = bval
  lres@tmXBOn                         = True
  lres@tmXBLabelsOn                   = True
  if ( resin .and. isatt(resin,"tmXBLabelFont") ) then   
    lres@tmXBLabelFont                = resin@tmXBLabelFont
  else
    lres@tmXBLabelFont                = _Font
  end if
  if ( resin .and. isatt(resin,"tmXBLabelFontHeightF") ) then 
    lres@tmXBLabelFontHeightF         = resin@tmXBLabelFontHeightF
  else
    lres@tmXBLabelFontHeightF         = _FontHGT
  end if
  lres@tmXBMajorOutwardLengthF        = 0.005*mprhgt
 
  lres@gsnFrame                       = False            ; Don't advance the frame
  xv = new ((/ncolors1+1,2/),float)
  yv = new ((/ncolors1+1,2/),float)
  th = new ((/ncolors1+1/),float)
 
  do i = 0, ncolors1  ;num_distinct_markers ;-1
    xv(i,0) = 0.00+i*2
    xv(i,1) = 0.00+i*2 

    yv(i,0) = 0.0  ;0.26
    yv(i,1) = 0.2  ;0.370
    th(i)   = 2.0      
  end do
  lres@xyDashPatterns                 = 0                         ; choose dash patterns
  lres@xyLineThicknesses              = th 
  lplot  = gsn_csm_xy (wks, xv, yv, lres)
 
;
;  Create logical variables to hold the marker and text resources.
;  These markers are different than the XY markers, because they are not
;  associated with an XY plot. You can put these markers on any plot.
;
  gsres               = True
  gsres@gsMarkerIndex = 16          ; Use filled dots for markers.
 
;
; Loop through each grouping of markers, and draw them one set at
; a time, assigning the proper color and size with gsn_marker.
; At the same time, draw a legend showing the meaning of the markers.
;
 
  do i = 0, num_distinct_markers-1
    if (.not.ismissing(lat_new(i,0)))
      gsres@gsMarkerColor      = colors(i)
      gsres@gsMarkerThicknessF = 0.005  
      gsres@gsMarkerSizeF      = 0.003 * sqrt(mprwdt)
      gsn_polymarker(wks, map, lon_new(i,:), lat_new(i,:), gsres)
    end if
  end do

  gsres@gsMarkerColor      =  1
  gsres@gsMarkerThicknessF =  8
  ix  = lres@vpXF + lres@vpWidthF/nbin*Mbin 
  iy  = lres@vpYF - lres@vpHeightF / 2.0
  gsn_polymarker_ndc(wks,ix, iy, gsres)
 
;;--------------------------------------------------------------------------------------------------
;; For the histogram
;;--------------------------------------------------------------------------------------------------
  hisres                                 = True
  hisres@vpXF                            = mapvpx 
  hisres@vpYF                            = hisvpy 
  hisres@vpWidthF                        = 0.90 * mprwdt 
  hisres@vpHeightF                       = hishgt
  hisres@gsnHistogramSelectNiceIntervals = False      ; intervals now float
  hisres@gsnHistogramComputePercentages  = False      ; change left axis to %
  hisres@tmXBLabelAngleF                 = 0.         ; change label angle
  hisres@gsnHistogramNumberOfBins        = nbin       ; create histogram with 10 bins 
  hisres@gsnHistogramBarWidthPercent     = 100.       ; Bar width is 100% of bin width
  hisres@tmXBLabelStride                 = 20         ; every other x-label
  hisres@tmYROn                          = False      ; no YR tick marks
  hisres@tmXTOn                          = False      ; no XT tick marks
  hisres@tmYLOn                          = False      ; no YL tick marks
  hisres@tmXBOn                          = False      ; no XB tick marks
  hisres@tiYAxisOn                       = False
  hisres@tmYLLabelsOn                    = False
  hisres@tmYRLabelsOn                    = False
 
  hisres@gsnDraw                         = True       ; Don't draw individual plot.
  hisres@gsnFrame                        = False      ; Don't advance frame.
  hisres@gsEdgesOn                       = False

  plot=gsn_histogram(wks, datin, hisres)              ; create histogram with 10 bins
 
;;;; for the output....
  iout  = 0
  return(iout)
end


