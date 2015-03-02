;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;------------------------------------------------------------------------
strs = (/"/home/fymonitor/MONITORFY3C/py2/plot/",sat,"_",instrument,"_","Nedt","_","CONFIG",".","ncl"/)
config_file = str_concat(strs)
print(config_file)

loadscript(config_file)

begin    
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 
    
    fil_in = file_in
    f_var  = addfile(fil_in, "r")
    
 
    dNedt    = f_var->Nedt
    dtime    = f_var->time    
 
   nnn         = 0
   nnn         = dimsizes(dtime(:,0)) 
   xaix            = ispan(0, nnn-1, 1)  
   print(xaix)
   timlab= dtime(:, 0)
   timlab = dtime(:, 0)*1000000 + dtime(:, 1)*10000 + dtime(:, 2)* 100+ dtime(:,3)
   print(timlab)
   xttt       = ind(mod(timlab, 10000) .eq. 100 .or. mod(timlab, 10000) .eq. 108)
   ;print(xttt)
   xmain          = xaix(xttt)   
   xmtt       = ind( mod(timlab, 200) .eq. 0 )
   xmino      = xaix(xmtt)
   caix       = sprinti("%4.4i",mod(timlab(xttt), 1000000)/100 )+"~C~"+sprinti("%4.4i", timlab(xttt)/1000000  )
  
     
   ymin_Nedt_data=ymin_Nedt(channel-1)
   ymax_Nedt_data=ymax_Nedt(channel-1)  
 
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
      res@tmXBValues                     = xmain
      res@tmXBLabels                     = caix
      res@tmXBMinorOn                    = True
      res@tmXBMinorValues                = xmino

;      res@trYMinF                        = 0 
;      res@trYMaxF                        = 3.0

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
      res@tmYLLabelFontHeightF           = 0.015
      res@tmYLLabelAngleF                = 0
      res@tmYLLabelFontThicknessF        = 2.0      
;      res@tmYLMode                       = "Explicit"
;      res@tmYLValues                     = ispan(160, 320, 20)
;      res@tmYLLabels                     = sprinti("%0.3i", ispan(160, 320, 20))       
;      res@tmYLMinorValues                = ispan(160, 320, 5)
;      res@tmYLMinorOn                    = True;


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
      
      
      res@trYMinF                        = ymin_Nedt_data
      res@trYMaxF                        = ymax_Nedt_data
      res@gsnLeftString                  = "a) NEdT"
      res@gsnCenterString                = "" 
    ;  res@gsnRightString                 = "10~S~5~N~   " 
      res@vpYF                           = 0.75
      plot(0)  = gsn_csm_xy(wks, xaix, dNedt , res)

     ; res@trYMinF                        = yminb
     ; res@trYMaxF                        = ymaxb
     ; res@gsnLeftString                  = "b) BIAS"
     ; res@gsnCenterString                = "" 
     ; res@gsnRightString                 = " K  "
     ; res@vpYF                           = 0.59
     ; print(bias)
     ; plot(1)  = gsn_csm_xy(wks, xaix, bias, res)  


      ;res@trYMinF                        = ymins 
      ;res@trYMaxF                        = ymaxs
      ;res@gsnLeftString                  = "c) STDD"
      ;res@gsnCenterString                = "" 
      ;res@gsnRightString                 = " K  "
      ;res@tmXBLabelsOn                   = False
      ;res@vpYF                           = 0.35
      ;res@tmXBLabelsOn                   = True
      ;print(stdd)
      ;plot(2)  = gsn_csm_xy(wks, xaix, stdd, res)         
  
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
        restx@txFontHeightF      = 0.016
        restx@txFont             = _Font
        restx@txFontThicknessF   = 2
        restx@txJust             = "CenterLeft"
        gsn_text_ndc (wks, LS1(i), lint*(i+1) + 0.07+ladd ,0.2,restx)        
        gsn_text_ndc (wks,file_title, 0.10,0.9 ,restx)     
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

