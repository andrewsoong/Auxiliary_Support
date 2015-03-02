;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;------------------------------------------------------------------------
;strs = (/"/home/fymonitor/MONITORFY3C/py2/plot/",sat,"_",instrument,"_","Nedt","_","CONFIG",".","ncl"/)
;config_file = str_concat(strs)
;print(config_file)

;loadscript(config_file)

begin    
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 
    
    fil_in = file_in
    f_var  = addfile(fil_in, "r")    
    
     
    TS   = f_var->TS(0,:,:)
   nnn  = dimsizes(TS&lat)   
   xp    = new( (/2*nnn/), float) 
   yp    = new( (/2*nnn/), float )    
  
 do k=0,nnn-1
   dx = sqrt(TS(k,{60}))  
   yp(k) =TS(k,{60}) + dx  
   xp(k) = TS&lat(k)     
   xp(2*nnn-1-k) = TS&lat(k)     
   yp(2*nnn-1-k) =TS(k,{60})- dx   
 end do 
 
    wks = gsn_open_wks("png", file_out)           ; open workstation
    plot  = new(1,graphic)
 ;  gsn_define_colormap(wks,"default")
	 res                    = True                    ; plot mods desired
	 res@gsnDraw            = False                   ; don't draw
	 res@gsnFrame           = False                   ; don't advance frame
	 res@xyLineThicknessF   = 2.                      ; line thickness
	 res@tfPolyDrawOrder    = "Predraw"               ; put line on top 
 	 res@vpYF                           = 0.75   
     res@vpXF                           = 0.10
     res@vpHeightF                      = 0.4
     res@vpWidthF                       = 0.6
     res@trYMinF                        = 220
     res@trYMaxF                        = 320
     res@trXMinF                        = -90 
     res@trXMaxF                        = 90
     res@txFontThicknessF               = 2
     res@xyMarkLineMode                 = "Lines"  ; "Markers", "MarkLines"
    
;;   For X-Top
      res@trXReverse                     = False        
      res@tmXTBorderOn                   = True
      res@tmXTOn                         = True
      res@tmXTLabelsOn                   = False
      
;;   For X-Bottom                         
      res@tmXBBorderOn                   = True
      res@tmXBOn                         = True
      res@tmXBLabelsOn                   = True
      res@tmXBLabelFont                  = 4
      res@tmXBLabelFontHeightF           = 0.015
      res@tmXBLabelAngleF                = 0
      res@tmXBLabelFontThicknessF        = 2.0
      res@tmXBMode                       = "Explicit"
    ;  res@tmXBValues                     = xaix
    ;  res@tmXBLabels                     = caix
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
      res@tmYLLabelFont                  = 4
      res@tmYLLabelFontHeightF           = 0.015
      res@tmYLLabelAngleF                = 0
      res@tmYLLabelFontThicknessF        = 2.0      
      res@gsnStringFont                  = 4
      res@gsnStringFontHeightF           = 0.015      
     
      plot  = gsn_csm_xy(wks,TS&lat, TS(:,{60}) , res) 
      
      txres               = True                      ; text mods desired
	 txres@txFontHeightF = 0.07                      ; text font height
	  
	 gsn_text(wks,plot,"A",0,240,txres)
      gsn_text(wks,plot,"B",40,300,txres) 
      
      gsres                   = True                        ; poly res
	 gsres@gsFillColor       = "SlateBlue"                 ; color chosen
	 dummy = gsn_add_polygon (wks,plot,xp,yp,gsres)	
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

