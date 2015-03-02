;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;------------------------------------------------------------------------
strs = (/"/home/fymonitor/MONITORFY3C/py2/plot/",sat,"_",instrument,"_","PRT4_4SUB","_","CONFIG",".","ncl"/)
config_file = str_concat(strs)
print(config_file)

loadscript(config_file)

begin    
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 
    
    fil_in = file_in
    f_var1 = addfile(fil_in, "r")   
    dprt4 = f_var1->prt4_3day   
    dtime = f_var1->time_3day
    
    mprt4 = f_var1->prt4_1month   
    mtime = f_var1->time_1month
    
    yprt4_avg = f_var1->prt4_avg_1year
    yprt4_max = f_var1->prt4_max_1year
    yprt4_min = f_var1->prt4_min_1year 
    yprt4_std = f_var1->prt4_std_1year 
    ytime = f_var1->prt4_time_1year 
       
    lprt4_avg = f_var1->prt4_avg_life
    lprt4_max = f_var1->prt4_max_life
    lprt4_min = f_var1->prt4_min_life
    lprt4_std = f_var1->prt4_std_life   
    ltime = f_var1->prt4_time_life 
    
   dtimlab = dtime(:, 0)
   dtimlab = dtime(:, 0)*1000000 + dtime(:, 1)*10000 + dtime(:, 2)* 100+ dtime(:,3)   
   
   mtimlab = mtime(:, 0)
   mtimlab = mtime(:, 0)*1000000 + mtime(:, 1)*10000 + mtime(:, 2)* 100+ mtime(:,3) 
   
   ytimlab = ytime(:, 0)
   ytimlab = ytime(:, 0)*1000000 + ytime(:, 1)*10000 + ytime(:, 2)* 100+ ytime(:,3) 
   
   ltimlab = ltime(:, 0)
   ltimlab = ltime(:, 0)*1000000 + ltime(:, 1)*10000 + ltime(:, 2)* 100+ ltime(:,3)       
 
 
   dlth = dimsizes(dtimlab) 
   dx = ispan(0, dlth-1, 1)
   mlth = dimsizes(mtimlab)
   mx = ispan(0, mlth-1, 1) 
   ylth = dimsizes(ytimlab) 
   yx = ispan(0, ylth-1, 1)
   llth = dimsizes(ltimlab)
   lx = ispan(0, llth-1, 1)  
   
   dy = new((/1,dlth/), "float", 1.0e+35)    
   dy(0, dx) = dprt4    
   
   my = new((/1,mlth/), "float", 1.0e+35)    
   my(0, mx) = mprt4   
      
   yy = new((/3,ylth/), "float", 1.0e+35)    
   yy(0, yx)    = yprt4_avg
   yy(1, yx)    = yprt4_max
   yy(2, yx)    = yprt4_min     
   
   
   ly = new((/3,llth/), "float", 1.0e+35)    
   ly(0, lx)    = lprt4_avg
   ly(1, lx)    = lprt4_max
   ly(2, lx)    = lprt4_min    
  
   
  dlthstep = (dlth-1)/5
  dxs = ispan(0, dlth-1, dlthstep)  
  dxlbs = sprinti("%4.4i",mod(dtimlab(dxs), 10000) )+"~C~"+sprinti("%4.4i", mod(dtimlab(dxs)/10000,10000)) 
 ;  dxlbs = sprinti("%4.4i",mod(dtimlab(dxs), 10000) )+"~C~"+sprinti("%4.4i", dtimlab(dxs)/10000) 
    
  mlthstep = (mlth-1)/5
  mxs = ispan(0, mlth-1, mlthstep) 
  mxlbs = sprinti("%4.4i",mod(mtimlab(mxs), 1000000)/100 )+"~C~"+sprinti("%4.4i", mtimlab(mxs)/1000000  ) 
  
  ylthstep = (ylth-1)/5
  yxs = ispan(0, ylth-1, ylthstep) 
  yxlbs = sprinti("%4.4i",mod(ytimlab(yxs), 1000000)/100 )+"~C~"+sprinti("%4.4i", ytimlab(yxs)/1000000  ) 
  
  llthstep = (llth-1)/5
  lxs = ispan(0, llth-1, llthstep) 
  lxlbs = sprinti("%4.4i",mod(ltimlab(lxs), 1000000)/100 )+"~C~"+sprinti("%4.4i", ltimlab(lxs)/1000000  )   
  
  
   yxp    = new( (/2*ylth/), integer, -9999) 
   yyp    = new( (/2*ylth/), float )   
  
 do k=0,ylth-1    
   yyp(k) = yprt4_avg(k) + yprt4_std(k) 
   yxp(k) = yx(k)   
   yxp(2*ylth-1-k) = yx(k)   
   yyp(2*ylth-1-k) = yprt4_avg(k) - yprt4_std(k)    
 end do 
 
 
   lxp    = new( (/2*llth/), integer, -9999) 
   lyp    = new( (/2*llth/), float )   
  
 do k=0,llth-1    
   lyp(k) = lprt4_avg(k) + lprt4_std(k) 
   lxp(k) = lx(k)   
   lxp(2*llth-1-k) = lx(k)   
   lyp(2*llth-1-k) = lprt4_avg(k) - lprt4_std(k)    
 end do 
 
   
   ;day_min=day_min(channel-1)
   ;day_max=day_max(channel-1)   
  ; mon_min=mon_min(channel-1)
  ; mon_max=mon_max(channel-1)   
   ;year_min=year_min(channel-1)
   ;year_max=year_max(channel-1)
   ;life_max=life_max(channel-1)
   ;life_min=life_min(channel-1)
   
   
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
     res@trYMinF                        = day_min
     res@trYMaxF                        = day_max 
     res@vpYF                           = 0.9     
     plot(0)  = gsn_csm_xy(wks, dx , dy , res)
     
    
     res@tmXBValues                     = mxs
     res@tmXBLabels                     = mxlbs
    ; res@tmXBMinorOn                    = True
    ; res@tmXBMinorValues                = mxmino
     res@gsnLeftString                  = sub_title(1)   
     res@trYMinF                        = mon_min
     res@trYMaxF                        = mon_max    
     res@vpYF                           = 0.68    
     plot(1)  = gsn_csm_xy(wks, mx , my , res) 
     
     res@tmXBValues                     = yxs
     res@tmXBLabels                     = yxlbs
    ; res@tmXBMinorOn                    = True
    ; res@tmXBMinorValues                = yxmino
     res@gsnLeftString                  = sub_title(2)
     res@trYMinF                        = year_min
     res@trYMaxF                        = year_max      
     res@vpYF                           = 0.46 
     res@tfPolyDrawOrder    = "Predraw"               ; put line on top      
     plot(2) = gsn_csm_xy(wks, yx , yy , res) 
      
     gsres                   = True                        ; poly res
     gsres@gsFillColor       = "SlateBlue"                 ; color chosen  
     dummy2 = gsn_add_polygon (wks,plot(2),yxp,yyp,gsres) 
      
     res@tmXBValues                     = lxs
     res@tmXBLabels                     = lxlbs
   ;  res@tmXBMinorOn                    = True
   ;  res@tmXBMinorValues                = lxmino   
     res@gsnLeftString                  = sub_title(3)    
 	 res@vpYF                           = 0.22     
     res@trYMinF                        = life_min
     res@trYMaxF                        = life_max   
     res@tfPolyDrawOrder    = "Predraw"               ; put line on top   
     plot(3) = gsn_csm_xy(wks, lx , ly , res)    
     
     gsres                   = True                        ; poly res
     gsres@gsFillColor       = "SlateBlue"                 ; color chosen  
     dummy3 = gsn_add_polygon (wks,plot(3),lxp,lyp,gsres)  
   
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

