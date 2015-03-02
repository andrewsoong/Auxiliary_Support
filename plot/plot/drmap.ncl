;------------------------------------------------------------------------
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;------------------------------------------------------------------------
begin
  fil_in = "/home/f3cmon/MONITOR/py/bin/FY3C_MWTS_GLOBAL_20140118_desc.HDF"
  f_var  = addfile(fil_in, "r")
 ; print(f_var)
  lat    = f_var->lat
  lon    = f_var->lon
  bts    = f_var->obs_bt
  srttov = f_var->sim_bt_rttov
  scrtm  = f_var->sim_bt_crtm
  drttov = f_var->diff_rttov
  dcrtm  = f_var->diff_crtm 
 ; print(dimsizes(lat))
  
    _Font      = 4 
    wks = gsn_open_wks("png","FY3C_MWTS_GLOBAL_20140118_desc.HDF")
    gsn_merge_colormaps (wks,"BlRe","BlAqGrYeOrRe")
  ; gsn_draw_colormap(wks)
    alphas             = (/"a)","b)","c)","d)","e) ","f) ","g) ","h) ","i) ", "j) ", "k) "/)     
    xyLVs              = (ispan(0, 12, 1) - 12)*1.5
   ; print(ispan(0, 24, 2)-12)
   ; print(xyLVs)
    xyLCs              = ispan(102,199, 7)
   ; print(xyLCs)

    xyLVs2             = (ispan(0, 12, 1) - 12)*0.3
    xyLCs2             = ispan(2, 99, 7)

    plot               = new(8,graphic)
    avgs  = floattoint(avg( bts(4,:,:)))/10*10
    print(avg( bts ))
    xyLVs = xyLVs + avgs+10
   ; print(xyLVs)
    
      do ifil = 0, 0
      res                                = True
      
  ;     res@tiMainString                   = "MWTS channel 3"
  ;    res@tiXAxisString                 = "xxx"       
 

      res@gsnMaximize                    = False               ; maximize pot in frame
      res@gsnFrame                       = False               ; don''t advance frame
      res@gsnDraw                        = False               ; don''t draw plot
      res@gsnPaperOrientation            = "portrait" 
      res@gsnAddCyclic                   = False    ; Data is not cyclic

      res@cnInfoLabelOn                  = False               ; Label needless
      res@cnFillOn                       = True               ; color Fill 
      res@cnFillMode                     = "CellFill"         ; Raster Mode
      res@cnRasterSmoothingOn            = True
      res@cnRasterMinCellSizeF           = 0.05
      res@cnLinesOn                      = False              ; Turn off contour lines
      res@cnLineLabelsOn                 = False              ; Turn off contour lines

      res@cnLevelSelectionMode           = "ExplicitLevels"     ; set manual contour levels
      res@cnMonoFillColor                = False 
      res@lbLabelBarOn                   = False 
      res@lbLabelFont                    = _Font
      res@cnLevels                       = xyLVs
      res@cnFillColors                   = xyLCs
      res@cnMissingValFillColor          = 0
      res@cnFillDrawOrder                = "PostDraw"

      res@trGridType                     = "TriangularMesh"

      res@mpDataBaseVersion              = "MediumRes"          ; Higher res coastline
      res@mpLimitMode                    = "LatLon"
      res@mpMinLatF                      = -90
      res@mpMaxLatF                      = 90
      res@mpMinLonF                      = 0
      res@mpMaxLonF                      = 360

      res@vpHeightF                      = 0.2
      res@vpWidthF                       = 0.4
 
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
      res@tmXBLabelFontHeightF           = 0.013      
      res@tmXBMode                       = "Explicit"
      res@tmXBValues                     = ispan(0,360,60)*1.0
      res@tmXBLabels                     = (/"0E","60E","120E","180","120W","60W","0W"/)

      res@tmXBMinorOn                    = True
      res@tmXBMinorValues                = ispan(0,360,10)*1.0

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
      res@tmYLLabelFontHeightF           = 0.013 
      res@tmYLMode                       = "Explicit"
      res@tmYLMinorValues                = ispan(-90,90,5) 
      res@tmYLMinorOn                    = True
      res@tmYLValues                     = (/-3,-2, -1, 0, 1, 2, 3/)*30
      res@tmYLLabels                     = (/"90S","60S","30S","Eq", "30N", "60N", "90N"/) 

      res@mpGridAndLimbOn                = False
      res@mpCenterLonF                   = 180         ; change map center
 
      res@gsnRightString                 = ""
      res@gsnLeftStringOrthogonalPosF    = 0.03 
;     res@gsnCenterString                = "Ascending"
      res@gsnStringFont                  = _Font
      res@gsnStringFontHeightF           = 0.013

      res1    = res
 
      res@sfXArray                      = lon(:, :)
      res@sfYArray                      = lat(:, :)  
      res@gsnLeftString                 = "a) OBS"
      res@vpXF                          = .06
      res@vpYF                          = .93      
      plot(0) = gsn_csm_contour_map(wks, bts(4, :, :), res)

      res@gsnLeftString                 = "b) OBS"
      res@vpXF                          = .55
      res@vpYF                          = .93
      plot(1) = gsn_csm_contour_map(wks, bts(4, :, :), res)

      res@gsnLeftString                 = "c) BGD"
      res@vpXF                          = .06
      res@vpYF                          = 0.65
      plot(2) = gsn_csm_contour_map(wks,srttov(4, :, :), res)

      res@gsnLeftString                 = "d) BGD"
      res@vpXF                          = .55
      res@vpYF                          = .65
      plot(3) = gsn_csm_contour_map(wks, scrtm(4, :, :), res)
     

      
      res@lbLabelBarOn                  = True
    ;  res@cnFillDrawOrder               = "PreDraw"
      plot(6) = gsn_csm_contour_map(wks, scrtm(4, :, :), res)

      x1   = 0.06
      x2   = 0.95
      y1   = 0.355
      y2   = 0.37
      labelbar_w_tri_ends(wks,plot(6), (/x1,x2/), (/y1,y2/))       


      res1@cnLevels                      = xyLVs2
      res1@cnFillColors                  = xyLCs2
      res1@sfXArray                      = lon(:, :)
      res1@sfYArray                      = lat(:, :)

      res1@gsnLeftString                 = "e) Difference"
      res1@vpXF                          = .06
      res1@vpYF                          = .31     
      plot(4) = gsn_csm_contour_map(wks, drttov(4, :, :), res1)

      res1@gsnLeftString                 = "f) Difference"
      res1@vpXF                          = .55
      res1@vpYF                          = .31
      plot(5) = gsn_csm_contour_map(wks, dcrtm(4, :, :), res1)
     
      res1@lbLabelBarOn                  = True
     ; res1@cnFillDrawOrder               = "PreDraw"
      plot(7) = gsn_csm_contour_map(wks, dcrtm(4, :, :), res1)
      x1   = 0.06
      x2   = 0.95
      y1   = 0.01
      y2   = 0.025
      labelbar_w_tri_ends(wks,plot(7), (/x1,x2/), (/y1,y2/))
      
    ;  delete(res)
      end do

       restx                    = True
        restx@txFontHeightF      = 0.018
        restx@txFont             = _Font
        restx@txJust             = "CenterLeft"

        gsn_text_ndc (wks, "MWTS Channel 3", 0.35 ,0.97 ,restx)

    draw(plot)
    frame(wks)
end
