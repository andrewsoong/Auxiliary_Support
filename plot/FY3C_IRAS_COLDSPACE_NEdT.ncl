;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
load  "/home/fymonitor/MONITORFY3C/py2/plot/GetMaxMinValue_lium.ncl"
;------------------------------------------------------------------------

begin    
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 
    
    fil_in = file_in
    f_var  = addfile(fil_in, "r")
    
 
    dcoldspace_nedt    = f_var->coldspace_nedt
    dtime    = f_var->time  
 
   nnn         = 0
   nnn         = dimsizes(dtime(:,0))
  ; print(nnn)
   
 ;  print(dimsizes(dtime(:,0))) 
   xaix            = ispan(0, nnn-1, 1) 
 
   mmm             = (nnn-1)/4
   xaixn           = ispan(0, nnn-1, mmm)
 

   timlab= dtime(:, 0)
   timlab= dtime(:, 0)*10000 + dtime(:, 1)*100 + dtime(:, 2) 
   ;print(timlab)
  
   cmaix = sprinti("%4.4i",mod(timlab(xaixn), 10000) )+"~C~"+sprinti("%4.4i", timlab(xaixn)/10000  ) 
     
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
   
    iout = ASGNbin_VALUE_opt( dcoldspace_nedt, resbin ) 
    dcoldspace_nedt_max = iout@max 
    dcoldspace_nedt_min = iout@min 
    dnbin = iout@nbin    
    dyval = fspan(dcoldspace_nedt_min,dcoldspace_nedt_max,dnbin)
    dyval_monor = fspan(dcoldspace_nedt_min,dcoldspace_nedt_max,dnbin*5)
   
 
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
      res@gsnPaperOrientation            = "portrait" 

      res@txFontThicknessF               = 2

      res@vpXF                           = 0.10
      res@vpHeightF                      = 0.4
      res@vpWidthF                       = 0.85

      res@xyMarkLineMode                 = "Lines"  ; "Markers", "MarkLines"
      res@xyLineThicknesses              = xyLTs    ; thicker line
      res@xyDashPatterns                 = xyDPs
      res@xyLineColors                   = xyLCs(:10)
      res@xyMarker                       = 16    ; 3 different markers
      res@xyMarkerColors                 = xyLCs(:10)
      res@xyMarkerSizeF                  = 0.005

      res@trXMinF                        = 0 
      res@trXMaxF                        = nnn - 1
 
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
      res@tmXBLabelFontHeightF           = 0.015
      res@tmXBLabelAngleF                = 0
      res@tmXBLabelFontThicknessF        = 2.0
      res@tmXBMode                       = "Explicit"
      res@tmXBValues                     = xaixn
      res@tmXBLabels                     = cmaix
      res@tmXBMinorOn                    = True
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
      res@tmYLLabelFontHeightF           = 0.010
      res@tmYLLabelAngleF                = 0
      res@tmYLLabelFontThicknessF        = 2.0 
       
      res@trYMinF                       = dcoldspace_nedt_min   
      res@trYMaxF                       = dcoldspace_nedt_max         
      res@tmYLMode                       = "Explicit"
      res@tmYLValues                     = dyval    
      res@tmYLLabels                     = sprintf("%6.3g", dyval)   
      res@tmYLMinorValues                = dyval_monor
      res@tmYLMinorOn                    = True
      
      res@gsnStringFont                  = _Font
      res@gsnStringFontHeightF           = 0.015           
;      res@gsnLeftStringOrthogonalPosF    = 0.03   

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
      res@tiYAxisFontHeightF             = 0.016
      res@tiYAxisFontThicknessF          = 2.0      
      res@tiYAxisOffsetYF                = 0.0        ;; positive to up
      res@tiYAxisOffsetXF                = -0.02      ;; positive to right
    
      res@gsnLeftString                  = "a) coldspace_nedt"
      res@gsnCenterString                = "" 
    ;  res@gsnRightString                 = "10~S~5~N~   " 
      res@vpYF                           = 0.75
      plot(0)  = gsn_csm_xy(wks, xaix, dcoldspace_nedt , res)
  
    end do 
      LS1  = (/"NEdT"/)     
      gsres                    = True
      gsres@gsMarkerIndex      = 16
      gsres@gsMarkerThicknessF = 2.0

      lint = 0.45
      ladd = -0.01
      do i = 0,0
        respl                    = True
        respl@gsLineThicknessF   = 2.0
        respl@gsLineColor        = xyLCs(i)
        respl@gsLineThicknessF   = xyLTs(i)*2.
        respl@gsLineDashPattern  = xyDPs(i)
        gsn_polyline_ndc(wks, (/-0.06,0.06/)+lint*(i+1)+ladd, (/0.2,0.2/), respl)
        ; gsn_polyline_ndc(wks, (/-0.03,0.03/)+lint*(i+1)+ladd, (/0.015,0.015/), respl)
        gsres@gsMarkerColor      = xyLCs(i)

        restx                    = True
        restx@txFontHeightF      = 0.014
        restx@txFont             = _Font
        restx@txFontThicknessF   = 2
        restx@txJust             = "CenterLeft"
        gsn_text_ndc (wks, LS1(i), lint*(i+1) + 0.07+ladd ,0.2,restx)        
        gsn_text_ndc (wks,file_title, 0.05,0.9 ,restx)     
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

