;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;------------------------------------------------------------------------
strs = (/"/home/fymonitor/MONITORFY3C/py2/plot/",sat,"_",instrument,"_","TURN","_","CONFIG",".","ncl"/)
config_file = str_concat(strs)
print(config_file)

loadscript(config_file)

begin    
    err = NhlGetErrorObjectId() 
    setvalues err 
      "errLevel" : "Fatal" ; only report Fatal errors 
    end setvalues 
 
;; get the number of days
    bg_ymd=BG_YMD
    ed_ymd=ED_YMD   
    bg_yer=bg_ymd/10000
    ed_yer=ed_ymd/10000
    bg_mon=mod(bg_ymd/100, 100)
    ed_mon=mod(ed_ymd/100, 100)
;    print(bg_yer+"  "+ed_yer+" "+bg_mon+" "+ed_mon)
    ndys = (/31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31/)
    nday = 0
    do iyer = bg_yer, ed_yer
      if ( isleapyear(iyer) ) then
        ndys(1) = 29
      else
        ndys(1) = 28
      end if
 
      imn1  = 1
      imn2  = 12
      if ( iyer .eq. bg_yer ) then
        imn1  = bg_mon
      end if
      if ( iyer .eq. ed_yer ) then
        imn2  = ed_mon
      end if

      do imon = imn1, imn2
        nday  = nday + ndys(imon - 1) 
      end do
    end do
;    print (nday)
    
    ntim    = 4
    timlab  = new((/1, nday*ntim/), integer, -9999)
    irec    = 0
    do iyer = bg_yer, ed_yer
      if ( isleapyear(iyer) ) then
        ndys(1) = 29
      else
        ndys(1) = 28
      end if
      
      imn1  = 1
      imn2  = 12
      if ( iyer .eq. bg_yer ) then
        imn1  = bg_mon
      end if

      if ( iyer .eq. ed_yer ) then
        imn2  = ed_mon
      end if
            
      do imon = imn1, imn2
        do iday = 1, ndys(imon - 1)
          do ihor = 0, ntim - 1
            timlab(0, irec) = iyer*1000000 + imon*10000 + iday*100 + ihor*6
            irec =irec + 1
          end do
        end do
      end do
      
    end do  
    ;print(timlab)
    ;exit
    
    
    fil_in = file_in
    f_var  = addfile(fil_in, "r")
 
    din11    = f_var->ira_turn1
    din12    = f_var->ira_turn2
    din13    = f_var->ira_turn3
    din14    = f_var->time
    timlab2 = din14(:, 0)
    timlab2 = din14(:, 0)*1000000 + din14(:, 1)*10000 + din14(:, 2)*100 + din14(:, 3)

    
    nnn     = dimsizes(timlab2)
    indxxx  = new(nnn, "integer", 0)
    do i = 0, nnn - 1
      indxxx(i)  = ind( timlab(0, :) .eq. timlab2(i) )
    end do
 
    turn01            = new((/2, nday*ntim/), "float", 1.0e+35)
    turn02            = turn01
    turn03            = turn01
    turn01(0, indxxx)    = din11
    turn02(0, indxxx)    = din12
    turn03(0, indxxx)    = din13

    

    ;num2           = numx 
    ;num2(0, :)  = numx(1, :)*10000.  - numx(0, :)*10000.
 
    xaix       = ispan(0, nday*ntim-1, 1) 

    if ( nday .gt. 600 ) then
      xttt       = ind(mod(timlab(0, :), 10000) .eq. 101 .or. mod(timlab(0, :), 10000) .eq. 401 .or. mod(timlab(0, :), 10000) .eq. 701 .or. mod(timlab(0, :), 10000) .eq. 1001 )      
      if ( mod( timlab(0, nday-1), 10000 ) .eq. 1231 .or. mod( timlab(0, nday-1), 10000 ) .eq. 331 .or. mod( timlab(0, nday-1), 10000 ) .eq. 630 .or. mod( timlab(0, nday-1), 10000 ) .eq. 930 ) then
        nnnn       = dimsizes(xttt)
        xind       = new(nnnn+1, "integer", -9999)
        xind(0:nnnn-1)  = xttt
        xind(nnnn) = nday-1 
      else       
        xind       = xttt
      end if
      caix       = sprinti("%4.4i",mod(timlab(0, xind), 10000) )+"~C~"+sprinti("%4.4i",timlab(0, xind)/10000 )
      xmain      = xaix(xind)      
      xmino      = ind(mod(timlab(0, :), 100) .eq. 1 .or. mod(timlab(0, :), 100) .eq. 15 )
    end if
 
    if ( nday .lt. 200 ) then
      xttt       = ind(mod(timlab(0, :), 10000) .eq. 100 .or. mod(timlab(0, :), 10000) .eq. 1500 )
      nnnn       = dimsizes(xttt)
      xind       = new(nnnn+1, "integer", -9999)
      xind(0:nnnn-1)  = xttt
      xind(nnnn) = nday*ntim-1
      delete(xttt)
      caix       = sprinti("%4.4i",mod(timlab(0, xind), 1000000)/100 )+"~C~"+sprinti("%4.4i", timlab(0, xind)/1000000  )
      xttt       = ind( mod(timlab(0, :), 200) .eq. 0 )
      xmain      = xaix(xind)
      xmino      = xaix(xttt)
    end if

  if ( nday .ge. 200 .and. nday .le. 600) then
      xttt       = ind(mod(timlab(0, :), 10000) .eq. 100 )
      nnnn       = dimsizes(xttt)
      xind       = new(nnnn+1, "integer", -9999)
      xind(0:nnnn-1)  = xttt
      xind(nnnn) = nday*ntim-1
      delete(xttt)
      caix       = sprinti("%4.4i",mod(timlab(0, xind), 1000000)/100 )+"~C~"+sprinti("%4.4i", timlab(0, xind)/1000000  )
      xttt       = ind( mod(timlab(0, :), 200) .eq. 0 )
      xmain      = xaix(xind)
      xmino      = xaix(xttt)
    end if
   yminn=ymin_turn1(0)
   ymaxn=ymax_turn1(0)
   yminb=ymin_turn2(0)
   ymaxb=ymax_turn2(0)
   ymins=ymin_turn3(0)
   ymaxs=ymax_turn3(0)
 
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

      res@vpXF                           = 0.2
      res@vpHeightF                      = 0.12     ;0.15
      res@vpWidthF                       = 0.68     ;0.85

      res@xyMarkLineMode                 = "Lines"  ; "Markers", "MarkLines"
      res@xyLineThicknesses              = xyLTs    ; thicker line
      res@xyDashPatterns                 = xyDPs
      res@xyLineColors                   = xyLCs(:10)
      res@xyMarker                       = 16    ; 3 different markers
      res@xyMarkerColors                 = xyLCs(:10)
      res@xyMarkerSizeF                  = 0.005

      res@trXMinF                        = 0 
      res@trXMaxF                        = nday*ntim - 1
 
;;   For X-Top
      res@trXReverse                     = False        
      res@tmXTBorderOn                   = True
      res@tmXTOn                         = True
      res@tmXTLabelsOn                   = False
      
;;   For X-Bottom                         
      res@tmXBBorderOn                   = True
      res@tmXBOn                         = True
      res@tmXBLabelsOn                   = False
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
      
      
      res@trYMinF                        = yminn 
      res@trYMaxF                        = ymaxn
      res@gsnLeftString                  = "a)ira_turn1"
      res@gsnCenterString                = "" 
      res@gsnRightString                 = "" 
      res@vpYF                           = 0.80
      res@tmXBLabelsOn                   = True
      plot(0)  = gsn_csm_xy(wks, xaix, turn01 , res)

      res@trYMinF                        = yminb
      res@trYMaxF                        = ymaxb
      res@gsnLeftString                  = "b)ira_turn2"
      res@gsnCenterString                = "" 
      res@gsnRightString                 = ""
      res@vpYF                           = 0.55
      res@tmXBLabelsOn                   = True
     ; print(bias)
      plot(1)  = gsn_csm_xy(wks, xaix, turn02, res)  


      res@trYMinF                        = ymins 
      res@trYMaxF                        = ymaxs
      res@gsnLeftString                  = "c)ira_turn3"
      res@gsnCenterString                = "" 
      res@gsnRightString                 = ""
      res@tmXBLabelsOn                   = False
      res@vpYF                           = 0.30
      res@tmXBLabelsOn                   = True
     ; print(stdd)
      plot(2)  = gsn_csm_xy(wks, xaix, turn03, res)         
  
    end do  
    

    LS   = (/"RTTOV","CRTM","CHN 3","CHN 4","Min Chan" /)
    gsres                    = True
    gsres@gsMarkerIndex      = 16
    gsres@gsMarkerThicknessF = 2.0

    lint = 0.3
    ladd = -0.03
    do i = 0, 1
      respl                    = True
      respl@gsLineThicknessF   = 2.0
      respl@gsLineColor        = xyLCs(i)
      respl@gsLineThicknessF   = xyLTs(i)*2.
      respl@gsLineDashPattern  = xyDPs(i)

      ;gsn_polyline_ndc(wks, (/-0.04,0.04/)+lint*(i+1)+ladd, (/0.08, 0.08/), respl)
      gsres@gsMarkerColor      = xyLCs(i)

      restx                    = True
      restx@txFontHeightF      = 0.016
      restx@txFont             = _Font
      restx@txFontThicknessF   = 2
      restx@txJust             = "CenterLeft"

      ;gsn_text_ndc (wks, LS(i), lint*(i+1) + 0.05+ladd ,0.08 ,restx)
      gsn_text_ndc (wks, file_title, 0.17,0.93 ,restx)
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