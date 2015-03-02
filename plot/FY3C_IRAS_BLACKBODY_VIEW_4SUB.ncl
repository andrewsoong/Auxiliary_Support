;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
load  "/home/fymonitor/MONITORFY3C/py2/plot/GetMaxMinValue_lium.ncl"
;------------------------------------------------------------------------

;------------------------------------------------------------------------
;process data
;------------------------------------------------------------------------ 
begin    
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 
    
    fil_in = file_in
    f_var1 = addfile(fil_in, "r")   
    dblackbody_view = f_var1->blackbody_view_3day   
    dtime = f_var1->blackbody_view_time_3day
    
    mblackbody_view = f_var1->blackbody_view_1month   
    mtime = f_var1->blackbody_view_time_1month
    
    yblackbody_view_avg = f_var1->blackbody_view_avg_1year
    yblackbody_view_max = f_var1->blackbody_view_max_1year
    yblackbody_view_min = f_var1->blackbody_view_min_1year 
    yblackbody_view_std = f_var1->blackbody_view_std_1year 
    ytime = f_var1->blackbody_view_time_1year 
       
    lblackbody_view_avg = f_var1->blackbody_view_avg_life
    lblackbody_view_max = f_var1->blackbody_view_max_life
    lblackbody_view_min = f_var1->blackbody_view_min_life
    lblackbody_view_std = f_var1->blackbody_view_std_life   
    ltime = f_var1->blackbody_view_time_life 
    
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
   dy(0, dx) = dblackbody_view    
   
   my = new((/1,mlth/), "double", 1.0e+35)    
   my(0, mx) = mblackbody_view   
      
   yy = new((/3,ylth/), "double", 1.0e+35)    
   yy(0, yx)    = yblackbody_view_avg
   yy(1, yx)    = yblackbody_view_max
   yy(2, yx)    = yblackbody_view_min     
   
   
   ly = new((/3,llth/), "double", 1.0e+35)    
   ly(0, lx)    = lblackbody_view_avg
   ly(1, lx)    = lblackbody_view_max
   ly(2, lx)    = lblackbody_view_min   
   
   yMaxMin = new(ylth*2, "double", 1.0e+35)  
   yMaxMin(0:ylth-1) = yblackbody_view_max
   yMaxMin(ylth:2*ylth-1) = yblackbody_view_min
   
   lMaxMin = new(llth*2, "double", 1.0e+35)  
   lMaxMin(0:llth-1) = lblackbody_view_max
   lMaxMin(llth:2*llth-1) = lblackbody_view_min
   
   
  dlthstep = (dlth-1)/5
  dxs = ispan(0, dlth-1, dlthstep)  
  dxlbs = sprinti("%4.4i",mod(dtimlab(dxs), 10000) )+"~C~"+sprinti("%4.4i", mod(dtimlab(dxs)/10000,10000)) 
 ;  dxlbs = sprinti("%4.4i",mod(dtimlab(dxs), 10000) )+"~C~"+sprinti("%4.4i", dtimlab(dxs)/10000) 
    
  mlthstep = (mlth-1)/5
  mxs = ispan(0, mlth-1, mlthstep) 
  mxlbs = sprinti("%4.4i",mod(mtimlab(mxs), 1000000)/100 )+"~C~"+sprinti("%4.4i", mtimlab(mxs)/1000000  ) 
  
  
  ylthstep = (ylth-2)/5
  yxs = ispan(0, ylth-2, ylthstep) 
  yxlbs = sprinti("%4.4i",mod(ytimlab(yxs), 1000000)/100 )+"~C~"+sprinti("%4.4i", ytimlab(yxs)/1000000  ) 
   
  llthstep = (llth-2)/5
  lxs = ispan(0, llth-2, llthstep) 
  lxlbs = sprinti("%4.4i",mod(ltimlab(lxs), 1000000)/100 )+"~C~"+sprinti("%4.4i", ltimlab(lxs)/1000000  )   
  
 
   yxp    = new( (/2*ylth/), integer, -9999) 
   yyp    = new( (/2*ylth/), float )   
  
 do k=0,ylth-1    
   yyp(k) = yblackbody_view_avg(k) + yblackbody_view_std(k) 
   yxp(k) = yx(k)   
   yxp(2*ylth-1-k) = yx(k)   
   yyp(2*ylth-1-k) = yblackbody_view_avg(k) - yblackbody_view_std(k)    
 end do 
 
 
   lxp    = new( (/2*llth/), integer, -9999) 
   lyp    = new( (/2*llth/), float )   
  
 do k=0,llth-1    
   lyp(k) = lblackbody_view_avg(k) + lblackbody_view_std(k) 
   lxp(k) = lx(k)   
   lxp(2*llth-1-k) = lx(k)   
   lyp(2*llth-1-k) = lblackbody_view_avg(k) - lblackbody_view_std(k)    
 end do
 
 ;------------------------------------------------------------------------
;GetMaxMinValue
;------------------------------------------------------------------------ 
    resbin       = True
    resbin@nbin  = 5
    resbin@nbin2add   = 1
    resbin@nbin2minus = 1
    resbin@Multiple = 1
    resbin@Const = 0
    resbin@DataRetain = 99.99   
   
    iout = ASGNbin_VALUE_opt( dblackbody_view, resbin ) 
    day_max = iout@max 
    day_min = iout@min 
    dnbin = iout@nbin    
   ; print("day_max: "+day_max+" "+"day_min"+" "+day_min+" "+dnbin)   
    dyval = fspan(day_min,day_max,dnbin)
    dyval_monor = fspan(day_min,day_max,dnbin*5)
   ; print(dyval) 
    
   
    iout = ASGNbin_VALUE_opt( mblackbody_view, resbin ) 
    mon_max = iout@max   
    mon_min = iout@min  
    mnbin = iout@nbin 
   ; print("mon_max: "+mon_max+" "+"mon_min"+" "+mon_min+" "+mnbin)   
    myval = fspan(mon_min,mon_max,mnbin)
    myval_monor = fspan(mon_min,mon_max,mnbin*5)
   ; print(myval)
  
    
    iout = ASGNbin_VALUE_opt( yMaxMin, resbin ) 
    year_max = iout@max 
    year_min = iout@min     
    ynbin =iout@nbin 
   ; print("year_max: "+year_max+" "+"year_min"+" "+year_min+" "+ynbin)  
    yyval = fspan(year_min,year_max,ynbin)
    yyval_monor = fspan(year_min,year_max,ynbin*5)
   ; print(yyval) 

   
    iout = ASGNbin_VALUE_opt( lMaxMin, resbin ) 
    life_max = iout@max   
    life_min = iout@min 
    lnbin =iout@nbin 
   ; print("life_max: "+life_max+" "+"life_min"+" "+life_min+" "+ynbin)    
    lyval = fspan(life_min,life_max,lnbin) 
    lyval_monor = fspan(life_min,life_max,lnbin*5) 
    
          
   
   
;------------------------------------------------------------------------
;draw plot
;------------------------------------------------------------------------   
   
   
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
      res@tmYLLabelFontThicknessF        = 2  
      

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
   
     
      res@tmYLBorderOn                   = True
      res@tmYLOn                         = True
      res@tmYLLabelsOn                   = True
      res@tmXBMode                       = "Explicit"
      
      resd                               = res
      resd@tmXBValues                     = dxs
      resd@tmXBLabels                     = dxlbs
    ; resd@tmXBMinorOn                    = True
    ; resd@tmXBMinorValues                = dxmino 
      resd@gsnLeftString                 = "a) Day"  
      resd@trYMinF                       = day_min   
      resd@trYMaxF                       = day_max      
      resd@tmYLMode                      = "Explicit"    
      resd@tmYLValues                    = dyval  
      resd@tmYLLabels                    = sprintf("%.4g", dyval)
      resd@tmYLMinorValues               = dyval_monor
      resd@tmYLMinorOn                   = True;
      resd@vpYF                          = 0.9     
      plot(0)  = gsn_csm_xy(wks, dx , dy , resd)
     
      resm                               =res
      resm@tmXBValues                    = mxs
      resm@tmXBLabels                    = mxlbs
    ; resm@tmXBMinorOn                   = True
    ; resm@tmXBMinorValues               = mxmino
      resm@gsnLeftString                 = "b) Month"   
      resm@trYMinF                       = mon_min
      resm@trYMaxF                       = mon_max    
      resm@tmYLMode                      = "Explicit"
      resm@tmYLValues                    = myval  
      resm@tmYLLabels                    = sprintf("%.4g", myval)
      resm@tmYLMinorValues               = myval_monor
      resm@tmYLMinorOn                   = True;    
      resm@vpYF                          = 0.68    
      plot(1)  = gsn_csm_xy(wks, mx , my , resm) 
     
      resy                                =res
      resy@tmXBValues                     = yxs
      resy@tmXBLabels                     = yxlbs
    ; resy@tmXBMinorOn                    = True
    ; resy@tmXBMinorValues                = yxmino
      resy@gsnLeftString                  = "c) Year"
      resy@trYMinF                        = year_min
      resy@trYMaxF                        = year_max   
      resy@tmYLMode                       = "Explicit"       
      resy@tmYLValues                     = yyval  
      resy@tmYLLabels                     = sprintf("%.4g", yyval)
      resy@tmYLMinorValues                = yyval_monor
      resy@tmYLMinorOn                    = True;   
      resy@vpYF                           = 0.46 
      resy@tfPolyDrawOrder    = "Predraw"               ; put line on top      
      plot(2) = gsn_csm_xy(wks, yx , yy , resy) 
      
      gsres                   = True                        ; poly res
      gsres@gsFillColor       = "SlateBlue"                 ; color chosen  
      dummy2 = gsn_add_polygon (wks,plot(2),yxp,yyp,gsres) 
      
      resl                               =res
      resl@tmXBValues                     = lxs
      resl@tmXBLabels                     = lxlbs
    ;  resl@tmXBMinorOn                    = True
    ;  resl@tmXBMinorValues                = lxmino   
      resl@gsnLeftString                  = "d) Life"    
 	    resl@vpYF                           = 0.22     
      resl@trYMinF                        = life_min
      resl@trYMaxF                        = life_max   
      resl@tmYLMode                       = "Explicit"
      resl@tmYLValues                     = lyval 
      resl@tmYLMinorValues                = lyval_monor
      resl@tmYLMinorOn                    = True;   
      resl@tmYLLabels                     = sprintf("%.4g", lyval)    
      resl@tfPolyDrawOrder    = "Predraw"               ; put line on top   
      plot(3) = gsn_csm_xy(wks, lx , ly , resl)    
     
      gsres                   = True                        ; poly res
      gsres@gsFillColor       = "SlateBlue"                 ; color chosen  
      dummy3 = gsn_add_polygon (wks,plot(3),lxp,lyp,gsres)  
   
      restx                    = True
      restx@txFontHeightF      = 0.014
      restx@txFont             = _Font
      restx@txFontThicknessF   = 2
      restx@txJust             = "CenterLeft"
      gsn_text_ndc (wks, file_title, 0.15,0.98 ,restx)   
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

