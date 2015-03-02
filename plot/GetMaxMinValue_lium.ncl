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
  
  if ( resin .and. isatt(resin,"Multiple") )  then 
    Multiple = resin@Multiple
  else
    Multiple = 1
  end if 
  
  
  if ( resin .and. isatt(resin,"Const") )  then 
    Const = resin@Const
  else
    Const = 0
  end if 
  
  nnbin    = nbin2a + nbin2m + 1
  nbinmax  = resin@nbin + nbin2a
 ; print(resin@nbin+"  "+nnbin+"  "+nbinmax)
  binmax   = new((/nnbin/), "float", 1.0e+35)

  datin    = datinx
;;;;
  avg1      = dim_avg(datin)
  min1      = min(datin)
  max1      = max(datin)
  dif1      = max1 - min1
;  print("stage1: "+avg1+" "+min1+" "+max1+" "+dif1)
  
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
;  print(datin) 

;;;;
  avg1      = dim_avg(datin)
  datin     = datin - avg1
  min1      = min(datin)
  max1      = max(datin)
  dif1      = max1 - min1
;  print("stage2: "+avg1+" "+min1+" "+max1+" "+dif1)
  
  if ( dif1 .eq. 0 ) then
     dif1   = 5
     min1   = min1 - 2
  end if
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
;  print("stage3: "+avg1+" "+min2+" "+max2+" "+dif2) 
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
 ; print(binx)
  min3   = min2
  max3   = binmax(iii)
 
  iout   = 0
  iout@avg1  = avg1 
  iout@bin   = binx
  iout@nbin  = nbin
  iout@max   = max3 * coef * Multiple + avg1 + Const
  iout@min   = min3 * coef * Multiple + avg1 - Const
  iout@iplc  = iplc
  iout@coef  = coef
;  print("max3: "+max3+" "+"min3"+" "+min3+" "+nbin)
  return(iout)
  
 
end
