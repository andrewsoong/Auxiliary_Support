;*********************************************
; lat_time_3.ncl
;*********************************************
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"

begin
;*********************************************
; read in data
;*********************************************
 fili  = "FY3B_MWTS_20131014_1200_to_20131231_0000_1LAT.HDF"                                ; data
; fili  = "60_180_real_data.HDF"
 
 f     = addfile (fili , "r")                         ; add file
; print(f)
; sst   = f->Stdp                                      ; sst anomalies
; print(dimsizes(sst))
;*********************************************
; manipulate data for plotting
;*********************************************
; lon90W  = ind(sst&lon.eq.270.)                      ; subscript at 90W

; shov      = sst({lon|270:270},lat|:,time|:)          ; put time last
; dims      = dimsizes(shov)                           

 sdemo     = f->avg_crtm
; print(dimsizes(sdemo))
 sdemo!0         = "lat"
 sdemo!1         = "time"
 nlat            = f->lat
 nlat@units = "degrees_north"
; print(nlat)
 sdemo&lat       = nlat
; print(sdemo&lat)
 ntime           = f->time
 nnn = dimsizes(ntime)
; print(nnn)
 xaix       = ispan(0, nnn-1, 1)
 print(xaix)  
; print(ntime)
; sdemo&time      = ntime(:,0)
 sdemo&time      = xaix
; sdemo@long_name = "SST"
; sdemo@units     = "C"
; print(sdemo)
                                   
 sdemo = smooth92d (sdemo,0.5, 0.25)                  ; 2D smoother
; cmaix = sprinti("%4.4i",ntime(::10,1))
 cmaix = sprinti("%4.4i",ntime(::30))
 print(cmaix)
 
;*************************
; plotting parameters
;*************************
 wks   = gsn_open_wks ("png", "lat_time_Avg" ) 

 gsn_define_colormap(wks,"ViBlGrWhYeOrRe")  ; choose color map

 res                  = True                ; plot mods desired
 res@cnFillOn         = True                ; color on
 res@lbLabelStride    = 4                   ; every other label
 res@lbOrientation    = "Vertical"          ; vertical label bar
 res@pmLabelBarOrthogonalPosF = -0.04       ; move label bar closer to axis
 res@cnLinesOn        = False               ; turn off contour lines
 res@gsnSpreadColors  = True                ; use full range of color map

 res@tmXBLabelFontHeightF           = 0.013
 res@tmXBMode                       = "Explicit"
 res@tmXBValues                     = ispan(0,nnn(0)-1,30)*1.0
; res@tmXBLabels                     = (/"2013101400","2013101900","2013102400","2013102900","2013110300","2013110800","2013111300"/)
 res@tmXBLabels                     = cmaix

 res@tiMainString     = "MWTS-Avg"  ; title

 res@vpXF             = 0.1                ; default is 0.2 (aspect ratio)
 res@vpYF             = 0.8                 ; default is 0.8
 res@vpHeightF        = 0.4                 ; default is 0.6
 res@vpWidthF         = 0.75                ; default is 0.6
  
;  res@cnLevelSelectionMode = "AutomaticLevels" ;
 res@cnLevelSelectionMode = "ManualLevels" ; manual levels
 res@cnMinLevelValF       =  -2 
 res@cnMaxLevelValF       =  0 
 res@cnLevelSpacingF      =  0.025 
 res@lbBoxLinesOn         = False
; res@lbLabelFont          = 0.013
 res@lbTitleOn            = False 

 plot = gsn_csm_lat_time(wks, sdemo, res) 

end
