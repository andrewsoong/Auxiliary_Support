;-------------------------------------------------------------
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;-------------------------------------------------------------
strs = (/"/home/fymonitor/MONITORFY3C/py2/plot/",sat,"_",instrument,"_","LAT","_","TIME","_","CRTM","_","CONFIG",".","ncl"/)
config_file = str_concat(strs)
print(config_file)

loadscript(config_file)

begin
;*********************************************
; read in data
;*********************************************
 fili  = file_in                                ; data
 
 f     = addfile (fili , "r")                         ; add file
;*********************************************
; manipulate data for plotting
;*********************************************
 sdemo     = f->avg_crtm
 sdemo1    = f->stdp_crtm
 sdemo!0         = "lat"
 sdemo!1         = "time"
 sdemo1!0        = "lat"
 sdemo1!1        = "time"

 nlat            = f->lat
 nlat@units = "degrees_north"
 sdemo&lat       = nlat
 sdemo1&lat      = nlat
; print(sdemo&lat)
 ntime           = f->time
 nnn             = dimsizes(ntime) 
 xaix            = ispan(0, nnn-1, 1)
 mmm             = (nnn-1)/4
 xaixn           = ispan(0, nnn-1, mmm)
; print(xaixn)  
; print(ntime)
; sdemo&time      = ntime(:,0)
 sdemo&time      = xaix
 sdemo1&time     = xaix
; sdemo@long_name = "SST"
; sdemo@units     = "C"
; print(sdemo)
                                   
 sdemo = smooth92d (sdemo,0.5, 0.25)                  ; 2D smoother
 sdemo1 = smooth92d (sdemo1,0.5, 0.25)                  ; 2D smoother
; cmaix = sprinti("%4.4i",ntime(::10,1))
; cmaix = sprinti("%4.4i",ntime(::30))
; cmaix = sprinti("%4.4i",ntime(::3))
 cmaix = sprinti("%4.4i",ntime(xaixn))
; print(cmaix)
 
;*************************
; plotting parameters
;*************************
 wks   = gsn_open_wks ("png", file_out) 
 gsn_define_colormap(wks,"BlAqGrYeOrRe")
; gsn_define_colormap(wks,"ViBlGrWhYeOrRe")  ; choose color map

 _Font      = 4 
 plot               = new(8,graphic)
 
 minavg = cnMin_avg(channel-1)
 maxavg = cnMax_avg(channel-1)
 spacavg = cnSpacing_avg(channel-1)
 minstd = cnMin_std(channel-1)
 maxstd = cnMax_std(channel-1)
 spacstd = cnSpacing_std(channel-1)

 do ifil = 0, 0
 res                  = True                ; plot mods desired
 
 res@gsnMaximize                    = False               ; maximize pot in frame
 res@gsnFrame                       = False               ; don''t advance frame
 res@gsnDraw                        = False               ; don''t draw plot
  

 res@cnFillOn         = True                ; color on
 res@lbLabelStride    = 4                   ; every other label
 res@lbOrientation    = "Vertical"          ; vertical label bar
 res@pmLabelBarOrthogonalPosF = -0.04       ; move label bar closer to axis
 res@cnLinesOn        = False               ; turn off contour lines
 res@gsnSpreadColors  = True                ; use full range of color map

 res@tmXBLabelFontHeightF           = 0.013
 res@tmXBMode                       = "Explicit"
; res@tmXBValues                     = ispan(0,nnn(0)-1,3)*1.0
; res@tmXBLabels                     = (/"2013101400","2013101900","2013102400","2013102900","2013110300","2013110800","2013111300"/)
 res@tmXBValues                     = xaixn
 res@tmXBLabels                     = cmaix
 res@tmXBMinorOn                    = True
  
; res@tiMainString     = "MWTS-Avg"  ; title

 res@vpHeightF                      = 0.28
 res@vpWidthF                       = 0.6

; res@vpXF             = 0.06                ; default is 0.2 (aspect ratio)
; res@vpYF             = 0.93                 ; default is 0.8
; res@vpHeightF        = 0.4                 ; default is 0.6
; res@vpWidthF         = 0.75                ; default is 0.6
  
; res@cnLevelSelectionMode = "AutomaticLevels" ;
 res@cnLevelSelectionMode = "ManualLevels" ; manual levels
 res@cnMinLevelValF       = minavg 
 res@cnMaxLevelValF       = maxavg 
 res@cnLevelSpacingF      = spacavg 
 res@lbBoxLinesOn         = False
 res@lbLabelFont          = 0.013 
 res@lbTitleOn            = False 
 res@lbLabelFontHeightF   = 0.012

 res@gsnLeftString                 = "a) avg"
 res@vpXF             = 0.2                ; default is 0.2 (aspect ratio)
 res@vpYF             = 0.8                 ; default is 0.8
; sdemo@units     = "avg"
 plot(0) = gsn_csm_lat_time(wks, sdemo, res) 

 ; res@cnLevelSelectionMode = "AutomaticLevels" ;
 res@cnLevelSelectionMode = "ManualLevels" ; manual levels
 res@cnMinLevelValF       =  minstd 
 res@cnMaxLevelValF       =  maxstd 
 res@cnLevelSpacingF      =  spacstd 
 res@lbBoxLinesOn         = False
 res@lbLabelFont          = 0.013 
 res@lbTitleOn            = False 
 res@lbLabelFontHeightF   = 0.012

 res@gsnLeftString                 = "b) std"
 res@vpXF                          = 0.2 
 res@vpYF                          = 0.35
 sdemo@units     = "stdp"
 plot(1) = gsn_csm_lat_time(wks, sdemo1, res) 
end do

 restx                    = True
 restx@txFontHeightF      = 0.014
 restx@txFontThicknessF   = 2
 restx@txFont             = _Font
 restx@txJust             = "CenterLeft"

 gsn_text_ndc (wks, file_title, 0.15 ,0.90 ,restx)


 draw(plot)
 frame(wks)
end

file_out_strs = (/file_out,".","png"/)
file_out_png  = str_concat(file_out_strs)
if (isfilepresent(file_out_png))
	print(">> "+file_out+".png is finish! >>")
	okstrs = (/file_out,".","png",".","OK"/)
	okline = str_concat(okstrs)
	header = (/"--------------------------------", \
	            okline, \
	            "--------------------------------"/)
	hlist = [/header/] 
	write_table(okline, "w", hlist, "%s ") 
else
   print(">> Failed to create "+file_out+".png ! >>")
end if
