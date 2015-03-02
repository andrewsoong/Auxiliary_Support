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
  print("stage1: "+avg1+" "+min1+" "+max1+" "+dif1)
  
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
  print("stage2: "+avg1+" "+min1+" "+max1+" "+dif1)
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

;;;; Generate the time lab from the beginning / ending time (mainly year)
undef("Gen_timlab")
function Gen_timlab(timbeg:numeric, timend:numeric )
local output
begin
 
;; get the number of days
  bg_ymd=timbeg
  ed_ymd=timend
;  print("BEG: "+bg_ymd+" END: "+ed_ymd)
 
  bg_yer=bg_ymd/10000
  ed_yer=ed_ymd/10000
  nyer  = ed_yer - bg_yer + 1
  bg_mon=mod(bg_ymd/100, 100)
  ed_mon=mod(ed_ymd/100, 100)
  nmon  = 13 - bg_mon + ed_mon + (nyer - 2)*12 
;  print("NYER: "+nyer+"  NMON: "+nmon)

  nday0 = 0
  do iyer = bg_yer, ed_yer
    if ( isleapyear(iyer) ) then
      ndys   = 366
    else
      ndys   = 365
    end if
    nday0    = nday0 + ndys
  end do
;  print("nday-0: "+nday0)

  timlab  = new(nday0, integer, -9999)
  irec    = 0
  do iyer = bg_yer, ed_yer
    if ( isleapyear(iyer) ) then
      ndys   = 366
    else
      ndys   = 365
    end if
    
    do idy = 1, ndys
      yddd = iyer*1000 + idy
      timlab(irec)  = yyyyddd_to_yyyymmdd(yddd)
      irec = irec + 1
    end do
  end do 

;;;; for dekads
  indxxx   = ind( mod(timlab, 100) .eq.  1 .or.  mod(timlab, 100) .eq. 11 .or. \
                  mod(timlab, 100) .eq. 21 )
  indyyy   = ind( timlab(indxxx) .ge. timbeg .and. timlab(indxxx) .le. timend )
  if( all( .not. ismissing(indyyy) ) ) then
    if ( mod(timbeg, 100) .eq. 1 .or. mod(timbeg, 100) .eq. 11 .or. \
         mod(timbeg, 100) .eq. 21 ) then
      ndkd       = dimsizes(indyyy) 
    else
      ndkd       = dimsizes(indyyy) + 1
    end if
  else
    ndkd         = 1
  end if
  delete(indxxx)
  delete(indyyy)
 
;;;; output
  output          = 0.0
  output@nday     = nday0
  output@ndkd     = ndkd
  output@nmon     = nmon
  output@nyer     = nyer
  output@timlab   = timlab
  return(output)
end

;;;;----------------------------------------------------------------------------------------------------------
;;;;    FOR DEKAD
;;;;----------------------------------------------------------------------------------------------------------
undef("ASGNtm_daylab_dkd")
function ASGNtm_daylab_dkd(timbeg:numeric, timend:numeric )
local output
begin
 
  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  timlab  = abcd@timlab

;;;; for dekads
  indxxx   = ind( mod(timlab, 100) .eq.  1 .or.  mod(timlab, 100) .eq. 11 .or. \
                  mod(timlab, 100) .eq. 21 )
  indyyy   = ind( timlab(indxxx) .ge. timbeg .and. timlab(indxxx) .le. timend )
  if( all( .not. ismissing(indyyy) ) ) then
    if ( mod(timbeg, 100) .eq. 1 .or. mod(timbeg, 100) .eq. 11 .or. \
         mod(timbeg, 100) .eq. 21 ) then
      j1         = indyyy(0)
      ndkd       = dimsizes(indyyy) 
    else
      j1         = indyyy(0) - 1 
      ndkd       = dimsizes(indyyy) + 1
    end if
    inddkd       = new( ndkd + 1, "integer", -999999 )
    inddkd(:ndkd-1)  = indxxx( j1:j1+ndkd-1 )
  else
    ndkd         = 1
    inddkd       = new( ndkd + 1, "integer", -999999 )
    inddkd(0)    = indxxx( maxind( ind( timlab(indxxx) .le. timbeg ) ) ) 
  end if
  indzzz         = ind( timlab(indxxx) .ge. timend )
  if ( all( ismissing(indzzz) ) ) then
    inddkd(ndkd) = nday0 - 1
  else
    inddkd(ndkd) = indxxx( indzzz(0) ) - 1
  end if
;  print("For Dekads: "+ndkd)
;  print("  "+inddkd+"  "+timlab(inddkd)) 
  delete(indxxx)
  delete(indyyy)
  delete(indzzz)

  j1        = 0
  j2        = ndkd
  otimlab   = timlab( inddkd(j1):inddkd(j2) )
  nday      = dimsizes(otimlab)
;  print(otimlab)
  oxmain    = inddkd(j1:j2) - inddkd(j1)
  ocmain    = sprinti("%4.4i",mod(otimlab(oxmain), 10000) )+"~C~"+sprinti("%4.4i", otimlab(oxmain)/10000 )
;  print(oxmain+" "+otimlab(oxmain)+" "+ocmain)
  oxmino    = ind(mod(otimlab, 1) .eq. 0 ) 
;  print(oxmino+"  "+otimlab(oxmino))
;  exit

;;;; for output
  output          = 0.0
  output@nday     = nday
  output@timlab   = otimlab
  output@iaxis    = ispan(0, nday - 1, 1)
  output@ntim     = ndkd
  output@vmain    = oxmain
  output@cmain    = ocmain
  output@vmino    = oxmino
;  print(output)
;  exit
  return(output) 
  
end

;;;;----------------------------------------------------------------------------------------------------------
;;;;    FOR EVERY MONTH
;;;;----------------------------------------------------------------------------------------------------------
undef("ASGNtm_daylab_mon")
function ASGNtm_daylab_mon(timbeg:numeric, timend:numeric )
local output
begin
 
  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  timlab  = abcd@timlab

;;;; for monthes
  indxxx   = ind( mod(timlab, 100) .eq.  1 )
  indyyy   = ind( timlab(indxxx) .ge. timbeg .and. timlab(indxxx) .le. timend )
  if( all( .not. ismissing(indyyy) ) ) then
    if ( mod(timbeg, 100) .eq. 1 ) then
      j1         = indyyy(0)
      nmon       = dimsizes(indyyy) 
    else
      j1         = indyyy(0) - 1 
      nmon       = dimsizes(indyyy) + 1
    end if
    indmon       = new( nmon + 1, "integer", -999999 )
    indmon(:nmon-1)  = indxxx( j1:j1+nmon-1 )
  else
    nmon         = 1
    indmon       = new( nmon + 1, "integer", -999999 )
    indmon(0)    = indxxx( maxind( ind( timlab(indxxx) .le. timbeg ) ) ) 
  end if
  indzzz         = ind( timlab(indxxx) .ge. timend )
  if ( all( ismissing(indzzz) ) ) then
    indmon(nmon) = nday0 - 1
  else
    indmon(nmon) = indxxx( indzzz(0) ) - 1
  end if
;  print("For Monthes: "+nmon)
;  print("  "+indmon+"  "+timlab(indmon)) 
  delete(indxxx)
  delete(indyyy)
  delete(indzzz)

  j1        = 0
  j2        = nmon
  otimlab   = timlab( indmon(j1):indmon(j2) )
  nday      = dimsizes(otimlab)
;  print(otimlab)
  oxmain    = indmon(j1:j2) - indmon(j1)
  ocmain    = sprinti("%4.4i",mod(otimlab(oxmain), 10000) )+"~C~"+sprinti("%4.4i", otimlab(oxmain)/10000 )
;  print(oxmain+" "+otimlab(oxmain)+" "+ocmain)
  oxmino    = ind( mod(otimlab, 10) .eq. 6 .or. mod(otimlab, 100) .eq. 1 .or. mod(otimlab, 100) .eq. 11 .or. \
                   mod(otimlab, 100) .eq. 21 )
;  print(otimlab(oxmino))
;  exit

;;;; for output
  output          = 0.0
  output@nday     = nday
  output@timlab   = otimlab
  output@iaxis    = ispan(0, nday - 1, 1)
  output@ntim     = nmon
  output@vmain    = oxmain
  output@cmain    = ocmain
  output@vmino    = oxmino
  return(output) 
  
end

;;;;----------------------------------------------------------------------------------------------------------
;;;;    FOR BI-MONTH
;;;;----------------------------------------------------------------------------------------------------------
undef("ASGNtm_daylab_bmn")
function ASGNtm_daylab_bmn(timbeg:numeric, timend:numeric )
local output
begin
 
  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  timlab  = abcd@timlab

;;;; for bi-monthes
  indxxx   = ind( mod(timlab, 100) .eq.  1 .and. mod( mod(timlab, 10000) / 100, 2 ) .eq.  1 )
  indyyy   = ind( timlab(indxxx) .ge. timbeg .and. timlab(indxxx) .le. timend )
  if( all( .not. ismissing(indyyy) ) ) then
    print( mod(timbeg/100, 2) )
    if ( mod(timbeg/100, 2) .eq. 1 ) then
      j1         = indyyy(0)
      nbmn       = dimsizes(indyyy) 
    else
      j1         = indyyy(0) - 1 
      nbmn       = dimsizes(indyyy) + 1
    end if
    indbmn       = new( nbmn + 1, "integer", -999999 )
    indbmn(:nbmn-1)  = indxxx( j1:j1+nbmn-1 )
  else
    nbmn         = 1
    indbmn       = new( nbmn + 1, "integer", -999999 )
    indbmn(0)    = indxxx( maxind( ind( timlab(indxxx) .le. timbeg ) ) ) 
  end if
  indzzz         = ind( timlab(indxxx) .ge. timend )
  if ( all( ismissing(indzzz) ) ) then
    indbmn(nbmn) = nday0 - 1
  else
    indbmn(nbmn) = indxxx( indzzz(0) ) - 1
  end if
;  print("For bmnthes: "+nbmn)
;  print("  "+indbmn+"  "+timlab(indbmn)) 
  delete(indxxx)
  delete(indyyy)
  delete(indzzz)

  j1        = 0
  j2        = nbmn
  otimlab   = timlab( indbmn(j1):indbmn(j2) )
  nday      = dimsizes(otimlab)
;  print(otimlab)
  oxmain    = indbmn(j1:j2) - indbmn(j1)
  ocmain    = sprinti("%4.4i",mod(otimlab(oxmain), 10000) )+"~C~"+sprinti("%4.4i", otimlab(oxmain)/10000 )
;  print(oxmain+" "+otimlab(oxmain)+" "+ocmain)
  oxmino    = ind( mod(otimlab, 100) .eq. 1 .or. mod(otimlab, 100) .eq. 11 .or. mod(otimlab, 100) .eq. 21 )
;  print(otimlab(oxmino))
;  exit

;;;; for output
  output          = 0.0
  output@nday     = nday
  output@timlab   = otimlab
  output@iaxis    = ispan(0, nday - 1, 1)
  output@ntim     = nbmn
  output@vmain    = oxmain
  output@cmain    = ocmain
  output@vmino    = oxmino
  return(output) 
  
end  

;;;;----------------------------------------------------------------------------------------------------------
;;;;    FOR EVERY SEASON
;;;;----------------------------------------------------------------------------------------------------------
undef("ASGNtm_daylab_ssn")
function ASGNtm_daylab_ssn(timbeg:numeric, timend:numeric )
local output
begin
 
  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  timlab  = abcd@timlab

;;;; for seasons
  indxxx   = ind( mod(timlab, 10000) .eq. 101 .or.  mod(timlab, 10000) .eq. 401 .or. \
                  mod(timlab, 10000) .eq. 701 .or.  mod(timlab, 10000) .eq. 1001 )
  indyyy   = ind( timlab(indxxx) .ge. timbeg .and. timlab(indxxx) .le. timend )
  if( all( .not. ismissing(indyyy) ) ) then
    if ( mod(timbeg, 10000) .eq. 101 .or.  mod(timbeg, 10000) .eq. 401 .or. \
         mod(timbeg, 10000) .eq. 701 .or.  mod(timbeg, 10000) .eq. 1001 ) then
      j1         = indyyy(0)
      nssn       = dimsizes(indyyy) 
    else
      j1         = indyyy(0) - 1 
      nssn       = dimsizes(indyyy) + 1
    end if
    indssn       = new( nssn + 1, "integer", -999999 )
    indssn(:nssn-1)  = indxxx( j1:j1+nssn-1 )
  else
    nssn         = 1
    indssn       = new( nssn + 1, "integer", -999999 )
    indssn(0)    = indxxx( maxind( ind( timlab(indxxx) .le. timbeg ) ) ) 
  end if
  indzzz         = ind( timlab(indxxx) .ge. timend )
  if ( all( ismissing(indzzz) ) ) then
    indssn(nssn) = nday0 - 1
  else
    indssn(nssn) = indxxx( indzzz(0) ) - 1
  end if
;  print("For seasons: "+nssn)
;  print("  "+indssn+"  "+timlab(indssn)) 
  delete(indxxx)
  delete(indyyy)
  delete(indzzz)

  j1        = 0
  j2        = nssn
  otimlab   = timlab( indssn(j1):indssn(j2) )
  nday      = dimsizes(otimlab)
;  print(otimlab)
  oxmain    = indssn(j1:j2) - indssn(j1)
  ocmain    = sprinti("%4.4i",mod(otimlab(oxmain), 10000) )+"~C~"+sprinti("%4.4i", otimlab(oxmain)/10000 )
;  print(oxmain+" "+otimlab(oxmain)+" "+ocmain)
  oxmino    = ind( mod(otimlab, 100) .eq. 1 .or. mod(otimlab, 100) .eq. 16 ) 
;  print(otimlab(oxmino))
;  exit

;;;; for output
  output          = 0.0
  output@nday     = nday
  output@timlab   = otimlab
  output@iaxis    = ispan(0, nday - 1, 1)
  output@ntim     = nssn
  output@vmain    = oxmain
  output@cmain    = ocmain
  output@vmino    = oxmino
  return(output) 
  
end

;;;;----------------------------------------------------------------------------------------------------------
;;;;    FOR EVERY HALF-YEAR
;;;;----------------------------------------------------------------------------------------------------------
undef("ASGNtm_daylab_hfy")
function ASGNtm_daylab_hfy(timbeg:numeric, timend:numeric )
local output
begin
 
  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  timlab  = abcd@timlab

;;;; for every half year
  indxxx   = ind( mod(timlab, 10000) .eq. 101 .or.  mod(timlab, 10000) .eq. 601 )
  indyyy   = ind( timlab(indxxx) .ge. timbeg .and. timlab(indxxx) .le. timend )
  if( all( .not. ismissing(indyyy) ) ) then
    if ( mod(timbeg, 10000) .eq. 101 .or.  mod(timbeg, 10000) .eq. 601 ) then
      j1         = indyyy(0)
      nhfy       = dimsizes(indyyy) 
    else
      j1         = indyyy(0) - 1 
      nhfy       = dimsizes(indyyy) + 1
    end if
    indhfy       = new( nhfy + 1, "integer", -999999 )
    indhfy(:nhfy-1)  = indxxx( j1:j1+nhfy-1 )
  else
    nhfy         = 1
    indhfy       = new( nhfy + 1, "integer", -999999 )
    indhfy(0)    = indxxx( maxind( ind( timlab(indxxx) .le. timbeg ) ) ) 
  end if
  indzzz         = ind( timlab(indxxx) .ge. timend )
  if ( all( ismissing(indzzz) ) ) then
    indhfy(nhfy) = nday0 - 1
  else
    indhfy(nhfy) = indxxx( indzzz(0) ) - 1
  end if
;  print("For Half-Year: "+nhfy)
;  print("  "+indhfy+"  "+timlab(indhfy)) 
  delete(indxxx)
  delete(indyyy)
  delete(indzzz)

  j1        = 0
  j2        = nhfy
  otimlab   = timlab( indhfy(j1):indhfy(j2) )
  nday      = dimsizes(otimlab)
;  print(otimlab)
  oxmain    = indhfy(j1:j2) - indhfy(j1)
  ocmain    = sprinti("%4.4i",mod(otimlab(oxmain), 10000) )+"~C~"+sprinti("%4.4i", otimlab(oxmain)/10000 )
;  print(oxmain+" "+otimlab(oxmain)+" "+ocmain)
  oxmino    = ind( mod(otimlab, 100) .eq. 1 ) 
;  print(otimlab(oxmino))
;  exit

;;;; for output
  output          = 0.0
  output@nday     = nday
  output@timlab   = otimlab
  output@iaxis    = ispan(0, nday - 1, 1)
  output@ntim     = nhfy
  output@vmain    = oxmain
  output@cmain    = ocmain
  output@vmino    = oxmino
  return(output) 
  
end

;;;;----------------------------------------------------------------------------------------------------------
;;;;    FOR EVERY YEAR
;;;;----------------------------------------------------------------------------------------------------------
undef("ASGNtm_daylab_yer")
function ASGNtm_daylab_yer(timbeg:numeric, timend:numeric )
local output
begin
 
  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  timlab  = abcd@timlab

;;;; for every year
  indxxx   = ind( mod(timlab, 10000) .eq. 101 )
  indyyy   = ind( timlab(indxxx) .ge. timbeg .and. timlab(indxxx) .le. timend )
  if( all( .not. ismissing(indyyy) ) ) then
    if ( mod(timbeg, 10000) .eq. 101 ) then
      j1         = indyyy(0)
      nyer       = dimsizes(indyyy) 
    else
      j1         = indyyy(0) - 1 
      nyer       = dimsizes(indyyy) + 1
    end if
    indyer       = new( nyer + 1, "integer", -999999 )
    indyer(:nyer-1)  = indxxx( j1:j1+nyer-1 )
  else
    nyer         = 1
    indyer       = new( nyer + 1, "integer", -999999 )
    indyer(0)    = indxxx( maxind( ind( timlab(indxxx) .le. timbeg ) ) ) 
  end if
  indzzz         = ind( timlab(indxxx) .ge. timend )
  if ( all( ismissing(indzzz) ) ) then
    indyer(nyer) = nday0 - 1
  else
    indyer(nyer) = indxxx( indzzz(0) ) - 1
  end if
;  print("For Year: "+nyer)
;  print("  "+indyer+"  "+timlab(indyer)) 
  delete(indxxx)
  delete(indyyy)
  delete(indzzz)

  j1        = 0
  j2        = nyer
  otimlab   = timlab( indyer(j1):indyer(j2) )
  nday      = dimsizes(otimlab)
;  print(otimlab)
  oxmain    = indyer(j1:j2) - indyer(j1)
  ocmain    = sprinti("%4.4i",mod(otimlab(oxmain), 10000) )+"~C~"+sprinti("%4.4i", otimlab(oxmain)/10000 )
;  print(oxmain+" "+otimlab(oxmain)+" "+ocmain)
  oxmino    = ind( mod(otimlab, 100) .eq. 1 ) 
;  print(otimlab(oxmino))
;  exit

;;;; for output
  output          = 0.0
  output@nday     = nday
  output@timlab   = otimlab
  output@iaxis    = ispan(0, nday - 1, 1)
  output@ntim     = nyer
  output@vmain    = oxmain
  output@cmain    = ocmain
  output@vmino    = oxmino
  return(output) 
  
end

;;;;----------------------------------------------------------------------------------------------------------
;;;;    FOR EVERY YEAR
;;;;----------------------------------------------------------------------------------------------------------
undef("ASGNtm_daylab_evy")
function ASGNtm_daylab_evy(timbeg:numeric, timend:numeric )
local output
begin
 
  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  timlab  = abcd@timlab

  nday       = dimsizes(timlab)
  otimlab    = timlab 
 
  oxmain     = ind( mod(otimlab, 100) .eq. 1 .or. mod(otimlab, 10000) .eq. 1231 )  
;  print(oxmain+" "+otimlab(oxmain))  
  ocmain     = sprinti("%4.4i",mod(otimlab(oxmain), 10000) ) 
  oxmino     = ind( mod(otimlab, 100) .eq. 1 .or. mod(otimlab, 100) .eq. 11  .or. mod(otimlab, 100) .eq. 21 )    
 
;;;; for output
  output          = 0.0
  output@nday     = nday
  output@timlab   = otimlab
  output@iaxis    = ispan(0, nday - 1, 1)
  output@ntim     = dimsizes(oxmain) - 1
  output@vmain    = oxmain
  output@cmain    = ocmain
  output@vmino    = oxmino
;  print(output)
  return(output) 
  
end
 
undef("ASGNtm_daylab_opt")
function ASGNtm_daylab_opt(timbeg:numeric, timend:numeric, draw_typ:string )
begin
  
;; get the number of days

  abcd    = Gen_timlab(timbeg, timend) 
  nday0   = abcd@nday
  ndkd0   = abcd@ndkd
  nmon0   = abcd@nmon
  nyer0   = abcd@nyer
  timlab  = abcd@timlab
;  print("timbeg: "+timbeg+" timend: "+timend)
;  print(nday0+" "+ndkd0+" "+nmon0+" "+nyer0)

;;;; display 6-ten-days of data  
  if ( draw_typ .eq. "MON" .or. ( draw_typ .eq. "ALL" .and. ndkd0 .le. 6 ) ) then
    print("Invoke: ASGNtm_daylab_dkd")
    abce  = ASGNtm_daylab_dkd( timbeg, timend )
    ntim  = abce@ntim
    j1    = where( ntim .gt. 6, ntim - 6, 0)
    j2    = ntim
  end if  
  
;;;; display 12-monthes of data
  if ( draw_typ .eq. "YER" .or. ( draw_typ .eq. "ALL" .and. nmon0 .le. 12 .and. ndkd0 .gt. 6 ) ) then
    if ( nmon0 .le. 6 ) then
      print("Invoke: ASGNtm_daylab_mon")
      abce  = ASGNtm_daylab_mon( timbeg, timend )
      ntim  = abce@ntim
      j1    = where( ntim .gt. 6, ntim - 6, 0)
      j2    = ntim
    else
      print("Invoke: ASGNtm_daylab_bmn")
      abce  = ASGNtm_daylab_bmn( timbeg, timend )
      ntim  = abce@ntim
      j1    = where( ntim .gt. 6, ntim - 6, 0)
      j2    = ntim  
    end if
  end if  

  if ( draw_typ .eq. "ALL" .and. nmon0 .gt. 12 ) then
    if ( nmon0 .lt. 18 ) then
      print("Invoke: ASGNtm_daylab_bmn")
      abce  = ASGNtm_daylab_bmn( timbeg, timend )
    else if( nmon0 .lt. 24 ) then
        print("Invoke: ASGNtm_daylab_ssn")
        abce  = ASGNtm_daylab_ssn( timbeg, timend )
      else
        print("Invoke: ASGNtm_daylab_yer")
        abce  = ASGNtm_daylab_yer( timbeg, timend )
      end if
    end if  
    ntim  = abce@ntim
    j1    = 0
    j2    = ntim
  end if
  
  if ( draw_typ .eq. "EVY" ) then
;    print("Invoke: ASGNtm_daylab_evy")
    abce  = ASGNtm_daylab_evy( timbeg, timend )
    j1    = 0
    j2    = abce@ntim
  end if  
  ntim    = j2 - j1

  output          = 0.0
  output@nday     = abce@vmain(j2) - abce@vmain(j1) + 1
  output@timlab   = abce@timlab(abce@vmain(j1):abce@vmain(j2))
  output@iaxis    = abce@iaxis(abce@vmain(j1):abce@vmain(j2)) - abce@iaxis(abce@vmain(j1)) 
;  print(output@iaxis+"  "+output@timlab )
;  print("NDAY: "+output@nday)
  output@ntim     = ntim
  output@vmain    = abce@vmain(j1:j2) - abce@iaxis(abce@vmain(j1)) 
  output@cmain    = abce@cmain(j1:j2)
;  print("J1: "+j1+" j2: "+j2)
;  print(abce@vmain(j1:j2)+"  "+abce@timlab(abce@vmain(j1:j2))+"  "+abce@cmain(j1:j2) )
;  print("vmino: ")
  indxxx          = ind( abce@vmino .ge. abce@vmain(j1) .and. abce@vmino .le. abce@vmain(j2) )
  output@vmino    = abce@vmino(indxxx) - abce@iaxis(abce@vmain(j1))
;  print(indxxx+"  "+output@vmino+"  "+output@timlab(output@vmino))
  return(output)
end


undef("ASGNtm_lzalab_opt")
function ASGNtm_lzalab_opt( nlza:numeric, nbin:numeric )
begin
  if ( nbin .eq. 5 ) then
    vmino   = ispan(0, nlza, 2)
    if ( nlza .eq. 98 ) then
      vmain = (/  0,   24,   48,   72, 97/)
      cmain = (/"01", "25", "49", "73", "98"/)    
    end if
  end if
  if ( nbin .eq. 8 ) then
    vmino   = ispan(0, nlza, 2)
    if ( nlza .eq. 56 ) then
      vmain = (/0, 8, 16, 24, 32, 40, 48, 55/)
      cmain = (/"01", "09", "17", "25", "33", "41", "49", "56"/)
    end if
  end if

  output            = 0
  output@vmain      = vmain
  output@cmain      = cmain
  output@vmino      = vmino
  return(output)

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
  print("NXX: "+nxxx+" NYY: "+nyyy)

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
  dltx      = BaseX * mprwdt
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
 
  mpres@vpXF                  = mapvpx
  mpres@vpYF                  = mapvpy
  mpres@vpWidthF              = 0.90 * mprwdt
  mpres@vpHeightF             = 0.45 * mprhgt
 
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
  if ( ixxx .eq. 0 ) then
    mpres@tmYLLabelsOn        = True
  else
    mpres@tmYLLabelsOn        = False
  end if
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


undef("udf_contour_timspt")
function udf_contour_timspt( wks:graphic, datin[*][*]:numeric, timlabx[*]:numeric, \
                             scanpntx[*]:numeric, resin:logical )
begin
;;;; 

;; resources that could be setted in the main script.
;  resin@nplot
;  resin@iplot
;  resin@nxxx
;  resin@nyyy
;  resin@ixxx
;  resin@iyyy
;

  if( .not. resin .or. .not. isatt(resin,"iplot") .or. .not. isatt(resin,"nplot") ) then
    print("The resource var (resin) has not been setted correctly, PLEASE CHECKE! EXIT!")
    exit
  end if
 
;; Get the distribution of plots
;;;; nxxx: number of columns of the paneled lots
;;;; nyyy: number of rows of the paneled lots
;;;;;; determined by the resource setted or calculated inner.
  if ( resin .and. isatt(resin, "nxxx" ) .and. isatt(resin, "nyyy" ) ) then
    nxxx      = resin@nxxx
    nyyy      = resin@nyyy
  else
    if ( resin@nplot .le. 4 ) then
      nxxx    = 1
      nyyy    = resin@nplot
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
  if ( resin .and. isatt(resin, "nxxx" ) .and. isatt(resin, "nyyy" ) ) then  
    ixxx      = resin@ixxx
    iyyy      = resin@iyyy
  else
    ixxx      = mod(resin@iplot , nxxx)
    iyyy      = resin@iplot / nxxx
  end if

  if ( ixxx .gt. nxxx .or. iyyy .gt. nyyy .or. nxxx*nyyy .lt. resin@nplot .or. resin@nplot .lt. resin@iplot  ) then
    print("nplot: "+resin@nplot+" iplot: "+resin@iplot)
    print("ixxx: "+ixxx+" iyyy: "+iyyy)
    print("nxxx: "+nxxx+" nyyy: "+nyyy)
    print("ERROR IN SETTING number/index of COLUMNS/ROWS PLOTTED, exit")
    iout      = 1
    exit
  end if
 
;;;; Get the Size of plot
;;;;;; The reference Size when one PLOT only. 
  BaseX     = 0.96
  BaseY     = 0.96

  mprhgt    = 1.000 / nyyy 
  mprwdt    = 1.000 / nxxx 
 
;;;;;; The distance between the left/top edges of two sub-plots 
  dltx      = BaseX * mprwdt
  dlty      = BaseY * mprhgt
  print("dltx: "+dltx+"dlty: "+dlty)

  if ( resin .and. isatt(resin, "lbOrientation" ) .and. resin@lbOrientation .eq. "Vertical" ) then
    print("to add.......")
  else
    maprtx    = 0.9
    maprty    = 0.52 
    dohghwdh  = True
    if ( resin .and. isatt(resin, "vpHeightF" ) .and. isatt(resin, "vpWidthF" ) ) then
      vpHGHF   = resin@vpHeightF
      vpWDHF   = resin@vpWidthF
      dohghwdh = False
      if ( vpHGHF .gt. 0.9*dlty .or. vpWDHF .gt. 0.9*dltx ) then
        dohghwdh = True
      end if
    end if
    if ( dohghwdh ) then
      vpWDHF  = dltx * maprtx 
      vpHGHF  = dlty * maprty
    end if 
;;;;;; Top-left corner for color map
    mapvpx    = (1.0 - BaseX) / 2.0 + ixxx*dltx + (1 - maprtx)*0.9*dltx 
    mapvpy    = (1.0 + BaseY) / 2.0 - iyyy*dlty - (1 - maprty)*0.2*dlty 
    print("mapvpx: "+mapvpx+" mapvpy: "+mapvpy)
    print("WIDTH: "+vpWDHF+" HEIGHT: "+vpHGHF)
;    exit

;;;;;; Position (Top-Left corner) of the Color bar in sub-plot  
    lb1vpy    = mapvpy - vpHGHF - (1 - maprty)*0.48*dlty
    lb1hgt    = 0.08 * vpHGHF
;;;;;; Position (Top-Left corner) of the Label of Color bar in sub-plot
    lb2vpy    = mapvpy - vpHGHF - (1 - maprty)*0.48*dlty
    lb2hgt    = 0.08 * vpHGHF
  
  end if
 
;;;;;; Position (Top-Left corner) of the map in sub-plot  

  print(vpHGHF+" "+vpWDHF+" "+mapvpx+" "+mapvpy+"  "+ixxx+"  "+iyyy)
  if ( resin .and. isatt(resin, "lbOrientation" ) .and. resin@lbOrientation .eq. "Vertical" ) then
    lbOrientation = "Vertical"
  else
    lbOrientation = "Horizontal"
  end if
 
  ymdbeg    = min(timlabx)
  ymdend    = max(timlabx)
  dxaxis    = ASGNtm_daylab_opt(ymdbeg, ymdend, "ALL")
  nday      = dxaxis@nday
  timlab    = dxaxis@timlab
  xiaxis    = dxaxis@iaxis
  ntim      = dxaxis@ntim
  xvmain    = dxaxis@vmain
  xcmain    = dxaxis@cmain
  xvmino    = dxaxis@vmino
;  print((/timlab/))
;  print((/xiaxis/))
;  print((/xvmain/))
;  print((/xcmain/))
;  print((/xvmino/))
  npnt      = dimsizes(scanpntx)

  _Font     = 4 
  _FontHGT  = 0.013 / sqrt( sqrt(1.0/mprwdt) ) 


;;;; for the color tables
  
;  xyLVs              = ispan(-5, 5, 1)*1.0 
;  xyLCs              = ispan(102, 199, 8)

  resbin       = True
  resbin@nbin  = 10
  resbin@nbin2add   = 2
  resbin@nbin2minus = 1
  iout = ASGNbin_VALUE_opt( ndtooned(datin), resbin )
;  print( iout@bin  )
;  print( iout@nbin )
;  print( iout@max  )
;  print( iout@min  ) 
  iplc      = iout@iplc 
  coef      = 10^iplc
  if ( iplc .ge. -1 .and. iplc .le. 1 ) then
    Rmin1     = iout@min * coef
    Rmax1     = iout@max * coef
  else
    Rmin1     = iout@min 
    Rmax1     = iout@max 
    datin     = datin / coef 
  end if
;  print(Rmin1+"  "+Rmax1)
;  exit
 
  nbin          = 180
  ncolors       = nbin+1
  ncolors1      = 11
  xyLVs         = fspan(Rmin1, Rmax1, nbin)
  xyLCs         = ispan(3, nbin+3, 1)

  gsn_define_colormap(wks,"BlAqGrYeOrReVi200")       ; choose colormap
  nc1 = NhlNewColor(wks,.8,.8,.8)        ; Add light gray to colormap,

  LeftStrs  = (/"a) ","b) ","c) ","d) ","e) ", "f) ", "g) ", "h) ","i) ", "j) "/)
 
  res                                = True
  res@gsnMaximize                    = False               ; maximize pot in frame
  res@gsnFrame                       = False               ; don''t advance frame
  res@gsnDraw                        = True               ; don''t draw plot
 
  res@cnInfoLabelOn                  = False               ; Label needless
  res@cnFillOn                       = True               ; color Fill 
  res@cnFillMode                     = "CellFill"         ; Raster Mode
  res@cnRasterSmoothingOn            = True
  res@cnRasterMinCellSizeF           = 0.05
  res@cnLinesOn                      = False              ; Turn off contour lines
  res@cnLineLabelsOn                 = False              ; Turn off contour lines

  res@lbLabelBarOn                   = False 
  res@lbLabelFont                    = _Font
  res@lbLabelFontHeightF             = 0.015
  res@lbOrientation                  = lbOrientation
  
  res@cnLevelSelectionMode           = "ExplicitLevels"     ; set manual contour levels
  res@cnMonoFillColor                = False 
  res@cnLevels                       = xyLVs
  res@cnFillColors                   = xyLCs
  res@cnMissingValFillColor          = 0
  res@cnFillDrawOrder                = "PostDraw"

  res@vpXF                           = mapvpx
  res@vpYF                           = mapvpy   
  res@vpHeightF                      = vpHGHF
  res@vpWidthF                       = vpWDHF
 
; or X-Top
  res@trXMinF                        = xiaxis(0)
  res@trXMaxF                        = xiaxis(dimsizes(xiaxis)-1)
  
  res@trXReverse                     = False        
  res@tmXTBorderOn                   = True
  res@tmXTOn                         = True
  res@tmXTLabelsOn                   = False
; or X-Bottom                         
  res@tmXBBorderOn                   = True
  res@tmXBOn                         = True
  res@tmXBLabelsOn                   = True
  res@tmXBLabelFont                  = _Font
  res@tmXBLabelFontHeightF           = 0.013      
  res@tmXBMode                       = "Explicit"
  res@tmXBValues                     = xvmain
  res@tmXBLabels                     = xcmain
  res@tmXBMinorOn                    = True
  res@tmXBMinorValues                = xvmino


  dyaxis    = ASGNtm_lzalab_opt( npnt, 5 )
  print(dyaxis)
  yvmain    = dyaxis@vmain
  ycmain    = dyaxis@cmain
  yvmino    = dyaxis@vmino
 
  res@trYMinF                        = -1.5
  res@trYMaxF                        = npnt + 0.5

; r Y-Right
  res@trYReverse                     = False
  res@tmYRBorderOn                   = True
  res@tmYROn                         = True
  res@tmYRLabelsOn                   = False
; r Y-Left      
  res@tmYLBorderOn                   = True
  res@tmYLOn                         = True
  res@tmYLLabelsOn                   = True
  res@tmYLLabelFont                  = _Font
  res@tmYLLabelFontHeightF           = 0.013 
  res@tmYLMode                       = "Explicit"
  res@tmYLMinorValues                = yvmino
  res@tmYLMinorOn                    = True
  res@tmYLValues                     = yvmain
  res@tmYLLabels                     = ycmain
  if ( resin .and. isatt(resin,"tmYLLabelFont") ) then
    res@tmYLLabelFont       = resin@tmYLLabelFont
  else
    res@tmYLLabelFont       = _Font
  end if
  if ( resin .and. isatt(resin,"tmYLLabelFontHeightF") ) then
    res@tmYLLabelFontHeightF  = resin@tmYLLabelFontHeightF
  else
    res@tmYLLabelFontHeightF  = _FontHGT
  end if
 
  if ( resin .and. isatt(resin,"gsnStringFont") ) then
    res@gsnStringFont               = resin@gsnStringFont
  else
    res@gsnStringFont               = _Font
  end if
  if ( resin .and. isatt(resin,"gsnStringFontHeightF") ) then
    res@gsnStringFontHeightF        = resin@gsnStringFontHeightF
  else
    res@gsnStringFontHeightF        = _FontHGT
  end if
  if ( resin .and. isatt(resin,"gsnLeftString") ) then 
    res@gsnLeftString               = resin@gsnLeftString
  else
    res@gsnLeftString               = LeftStrs( resin@iplot )
  end if
  res@gsnLeftStringOrthogonalPosF     = -0.03 
  res@gsnLeftStringParallelPosF       = 0.01
  res@gsnCenterStringOrthogonalPosF   = -0.03
  if ( resin .and. isatt(resin,"gsnCenterString") ) then 
    res@gsnCenterString               = resin@gsnCenterString
  else
    res@gsnCenterString               = ""
  end if
  res@gsnRightStringOrthogonalPosF   = -0.03 
  if ( iplc .ge. -1 .and. iplc .le. 1 ) then
    res@gsnRightString               = ""
  else
    res@gsnRightString               = "x10~S~"+iplc+"~N~  "
  end if
 
  print("res@vpXF: "+res@vpXF+" res@vpYF: "+res@vpYF)
;  res@cnLevels                      = xyLV2s
;  res@lbLabelStrings                = sprintf("%2.1f", xyLV2s) 
  plot = gsn_csm_contour(wks, datin, res)
;  print(stddev(numxout))

;;;; label bar - colors 
  lbres                               = True
  lbres@vpXF                          = mapvpx
  lbres@vpYF                          = lb1vpy
  lbres@vpWidthF                      = vpWDHF          ; Width of labelbar
  lbres@vpHeightF                     = lb1hgt          ; Height of labelbar
  lbres@lbAutoManage                  = True
  lbres@lbOrientation                 = "Horizontal"
  lbres@lbMonoFillPattern             = True
  lbres@lbFillColors                  = xyLCs          ; ispan(0,ncolors-1,1)                                  
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
  lres@vpWidthF                       = vpWDHF      ; Width of labelbar
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
 
  iout   = 0
  return(iout)
end

