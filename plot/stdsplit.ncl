;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;------------------------------------------------------------------------
strs = (/"/home/fymonitor/MONITORFY3C/py2/plot/",sat,"_",instrument,"_","COLD_FOUR","_","CONFIG",".","ncl"/)
config_file = str_concat(strs)
print(config_file)

loadscript(config_file)

begin    
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 
    
    
    f_var1 = addfile(file1, "r")    
    f_var2 =  addfile(file2, "r")
    Avg   = f_var1->Avg
    Max   = f_var1->Max
    Min   = f_var1->Min
    dtime = f_var1->time 
    Nedt  = f_var2->Nedt
    Ntime = f_var2->time   
    
   timlab = dtime(:, 0)
   timlab = dtime(:, 0)*1000000 + dtime(:, 1)*10000 + dtime(:, 2)* 100+ dtime(:,3)   
 
   Ntimlab = Ntime(:, 0)
   Ntimlab = Ntime(:, 0)*1000000 + Ntime(:, 1)*10000 + Ntime(:, 2)* 100+ Ntime(:,3)   
  
   nnn         = dimsizes(timlab)
   xaix        = ispan(0, nnn-1, 1)
     
   Nnnn         = dimsizes(Ntimlab)
   Nxaix        = ispan(0, Nnnn -1, 1)  
      
   numx        = new((/3, nnn/), "float", 1.0e+35)    
   numx(0, xaix)    = Avg
   numx(1, xaix)    = Max
   numx(2, xaix)    = Min   
   
   Nearray        = new((/1, Nnnn/), "float", 1.0e+35)    
   Nearray(0, Nxaix)    = Nedt   
    
   mmm = (nnn-1)/4
   xaixn = ispan(0, nnn-1, mmm ) 
  caix  = sprinti("%4.4i",mod(timlab(xaixn ), 1000000)/100 )+"~C~"+sprinti("%4.4i", timlab(xaixn)/1000000  ) 
 
   xp    = new( (/2*nnn/), integer, -9999) 
   yp    = new( (/2*nnn/), float )   
   nxp         = dimsizes(xp) 
   xaixp        = ispan(0, nxp-1, 1)  
 do k=0,nnn-1
   dx = sqrt(Avg(k))  
   yp(k) = Avg(k) + dx 
   xp(k) = xaix(k)   
   xp(2*nnn-1-k) = xaix(k)   
   yp(2*nnn-1-k) = Avg(k) - dx    
 end do 
   
   day_min=day_min(channel-1)
   day_max=day_max(channel-1)
   mon_min=mon_min(channel-1)
   mon_max=mon_max(channel-1)
   year_min=year_min(channel-1)
   year_max=year_max(channel-1)
   life_max=life_max(channel-1)
   life_min=life_min(channel-1)
   
   
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
      
;;   For X-Bottom                         
      res@tmXBBorderOn                   = True
      res@tmXBOn                         = True
      res@tmXBLabelsOn                   = True
      res@tmXBLabelFont                  = _Font
      res@tmXBLabelFontHeightF           = 0.01
      res@tmXBLabelAngleF                = 0
      res@tmXBLabelFontThicknessF        = 2.0
      res@tmXBMode                       = "Explicit"
     res@tmXBValues                     = xaixn
      res@tmXBLabels                     = caix
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
   
    
     res@gsnLeftString                  = "a) NEdT"   
     res@trYMinF                        = day_min
     res@trYMaxF                        = day_max 
     res@vpYF                           = 0.9     
     plot(0)  = gsn_csm_xy(wks, xaix , numx , res)
     
     res@gsnLeftString                  = "a) NEdT"   
     res@trYMinF                        = mon_min
     res@trYMaxF                        = mon_max    
     res@vpYF                           = 0.68    
     plot(1)  = gsn_csm_xy(wks, Nxaix , Nearray , res) 
     
     res@gsnLeftString                  = "a) NEdT"
     res@trYMinF                        = year_min
     res@trYMaxF                        = year_max      
     res@vpYF                           = 0.46 
     res@tfPolyDrawOrder    = "Predraw"               ; put line on top      
     plot(2) = gsn_csm_xy(wks, xaix , numx , res) 
      
     gsres                   = True                        ; poly res
     gsres@gsFillColor       = "SlateBlue"                 ; color chosen  
     dummy = gsn_add_polygon (wks,plot(2),xp,yp,gsres) 
        
     res@gsnLeftString                  = "a) NEdT"    
 	 res@vpYF                           = 0.22     
     res@trYMinF                        = life_min
     res@trYMaxF                        = life_max   
     res@tfPolyDrawOrder    = "Predraw"               ; put line on top   
     plot(3) = gsn_csm_xy(wks, xaix , numx , res)    
     
     gsres                   = True                        ; poly res
     gsres@gsFillColor       = "SlateBlue"                 ; color chosen  
     dummy = gsn_add_polygon (wks,plot(3),xp,yp,gsres)  
     
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

