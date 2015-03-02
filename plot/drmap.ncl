;------------------------------------------------------------------------
load  "/home/f3cmon/ncl/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "/home/f3cmon/ncl/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "/home/f3cmon/ncl/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "/home/f3cmon/ncl/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "/home/f3cmon/ncl/lib/UDF.ncl"
;------------------------------------------------------------------------
begin
  nchn=24
 ; print(nchn)
  cnvr=24
  midvr= cnvr/2
 ; print(midvr)
 ; print(cnvr)
  cnvsp=2
 ; print(cnvsp)
  cnvsu=12
 ; print(cnvsu)
  cnva=0
 ; print(cnva)

  fil_in = "/home/f3cmon/DATA/TMP/GLOBAL/FY3C_IRAS_GLOBAL_20140220_CH20_PM.HDF"
  f_var  = addfile(fil_in, "r")
 ; print(f_var)
  scans  = f_var->scans
 ; print(scans)
  lat    = f_var->lat
  lon    = f_var->lon
  bts    = f_var->obs_bt
  srttov = f_var->sim_bt_rttov
  scrtm  = f_var->sim_bt_crtm
  drttov = f_var->diff_rttov
  dcrtm  = f_var->diff_crtm 
 ; print(dimsizes(lat))

   dimen_bts = new((/1, 2/), "integer", -9999)
   dimen_bts=dimsizes(bts(:,:))
 ;  print(dimen_bts)
   ;ntot = 4770
   ntot = dimen_bts(0,0)
 ;  print(ntot)
   dimen_scans = new((/1, 2/), "integer", -9999)
   dimen_scans = dimsizes(scans)
 ;  print(dimen_scans)
   nfil1 = dimen_scans(0,0)
  ; nfil1 = 2
  ; npnt = 90
   npnt = dimen_bts(0,1)
 ;  print(npnt)
   print(dimen_scans(0,0))
 ;  print(nfil1)
   bts1    = new((/ntot + nfil1 * 10, npnt/), "float", 1.0e+35)
 ;  print(dimsizes(bts1)) 
   dday    = bts1
   dhor    = bts1
   dmin    = bts1
   dlat    = bts1
   dlon    = bts1
   dsrttov = bts1
 ;  print(dimsizes(dsrttov))
   dscrtm   = bts1
   ddrttov = bts1
   ddcrtm   = bts1
   
   i1      = 0
   do ifil = 0, nfil1 - 1
 ;  print(ifil)
   ;scans=f_var->scans
 ;  print(scans(ifil))
   nscn = scans(ifil)
 ;  print(nscn)
   i2      = i1 + nscn - 1
 ;  print(i2)
   time = f_var->time
   ;print(dimsizes(time))
   
   ;print(20)
   ; CHN =20 - 1
   ; print(CHN)   
   
  ; dday(i1:i2, :)   = time(2, i1:i2, :)
  ; print(dimsizes(time(2, i1:i2, :)))
  ;  print(dimsizes(dday(i1:i2, :)))
  ; dhor(i1:i2, :)   = time(3, i1:i2, :)
  ; dmin(i1:i2, :)   = time(4, i1:i2, :)
   dlat(i1:i2, :)   = lat(i1:i2,:)
   ;print(dimsizes(lat(i1:i2,:)))
   dlon(i1:i2, :)   = lon(i1:i2,:)
  ; bts1(i1:i2, :)   = bts(CHN, i1:i2, :)
  ; print(bts1(i1:i2, :))
  ; dsrttov(i1:i2, :) = srttov(CHN, i1:i2, :)   
  ; dscrtm(i1:i2, :) = scrtm(CHN, i1:i2, :)
  ; ddrttov(i1:i2, :)= drttov(CHN, i1:i2, :)
  ; ddcrtm(i1:i2, :) = dcrtm(CHN, i1:i2, :)
    bts1(i1:i2, :)   = bts(i1:i2, :)
  ; print(bts1(i1:i2, :))
    dsrttov(i1:i2, :) = srttov(i1:i2, :)   
    dscrtm(i1:i2, :) = scrtm(i1:i2, :)
    ddrttov(i1:i2, :)= drttov(i1:i2, :)
    ddcrtm(i1:i2, :) = dcrtm(i1:i2, :)

;; kick out overlaped data
     if ( ifil .eq. 0 ) then
  ;     hor0     = dhor(0, 0)
  ;     print(hor0)
  ;     timref   = ( dhor(i2, 0) - hor0 )*60 + dmin(i2, 0)
       timref   = time(i2,0)- time(0,0)
       latref   = dlat(i2, npnt/2)
  ;     print(latref)
      ; print(timref)
     else
      ; indfil   = ind( ( dhor(i1:i2, 0) - hor0 )*60 + dmin(i1:i2, 0) .le. timref .and. dlat(i1:i2, npnt/2) .gt. latref )
        indfil   = ind( (time(i1:i2,0) - time(0,0)).le. timref .and. dlat(i1:i2, npnt/2) .gt. latref )
      ; print(indfil)
       if ( all( .not. ismissing(indfil) ) ) then
        indfil = indfil + i1
      ;  print(indfil)
        dlat(indfil, :)  = dlat@_FillValue
        dlon(indfil, :)  = dlon@_FillValue
      ;  print(dlat@_FillValue)
       end if
        timref   = time(i2,0)- time(0,0)
      ;  print(timref)
        latref   = dlat(i2, npnt/2)
        delete(indfil)
        
     end if
      i1      = i1 + nscn ;+ 10   
   end do 
    _Font      = 4 
    wks = gsn_open_wks("png","FY3C_IRAS_CH20_20140416_192503")
    gsn_merge_colormaps (wks,"BlRe","BlAqGrYeOrRe")
  ; gsn_draw_colormap(wks)
    alphas             = (/"a)","b)","c)","d)","e) ","f) ","g) ","h) ","i) ", "j) ", "k) "/)     
  ;  xyLVs              = (ispan(0, 12, 1) - 12)*1.5
    xyLVs              = (ispan(0, cnvr, cnvsp) - cnvsu)*1.5
   ; print(ispan(0, 24, 2)-12)
     print(xyLVs)
    xyLCs              = ispan(102,199, 7)
   ; print(xyLCs)

    xyLVs2             = (ispan(0, 12, 1) - 6)*0.5
  ;  xyLCs2             = ispan(2, 99, 7)
    xyLCs2             = xyLCs

    plot               = new(8,graphic)
    ;print(20)
   ; CHN =20 - 1
   ; print(CHN)
   ; avgs  = floattoint(avg( bts(CHN,:,:)))/10*10
     avgs  = floattoint(avg( bts1 ))
   ; print(avg( bts ))
     print(avgs)
    xyLVs = xyLVs + avgs+ cnva
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

      res@txFontThicknessF               = 2.
 
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
      res@tmXBLabelFontThicknessF        = 2.      
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
      res@tmYLLabelFontThicknessF        = 2.
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
 
      res@sfXArray                      = dlon(:, :)
      res@sfYArray                      = dlat(:, :)  
      res@gsnLeftString                 = "a) OBS"
      res@vpXF                          = .06
      res@vpYF                          = .93      
    ;  plot(0) = gsn_csm_contour_map(wks, bts(CHN, :, :), res)
      plot(0) = gsn_csm_contour_map(wks, bts1, res)

      res@gsnLeftString                 = "b) OBS"
      res@vpXF                          = .55
      res@vpYF                          = .93
   ;   plot(1) = gsn_csm_contour_map(wks, bts(CHN, :, :), res)
      plot(1) = gsn_csm_contour_map(wks, bts1, res)

      res@gsnLeftString                 = "c) BGD"
      res@vpXF                          = .06
      res@vpYF                          = 0.65
    ;  plot(2) = gsn_csm_contour_map(wks,srttov(CHN, :, :), res)
      plot(2) = gsn_csm_contour_map(wks,dsrttov, res)

      res@gsnLeftString                 = "d) BGD"
      res@vpXF                          = .55
      res@vpYF                          = .65
     ; plot(3) = gsn_csm_contour_map(wks, scrtm(CHN, :, :), res)
     ; plot(3) = gsn_csm_contour_map(wks, dscrtm, res)
     

      
      res@lbLabelBarOn                  = True
    ;  res@cnFillDrawOrder               = "PreDraw"
    ;  plot(6) = gsn_csm_contour_map(wks, scrtm(CHN, :, :), res)
       plot(6) = gsn_csm_contour_map(wks, dscrtm, res)

      x1   = 0.06
      x2   = 0.95
      y1   = 0.355
      y2   = 0.37
      labelbar_w_tri_ends(wks,plot(6), (/x1,x2/), (/y1,y2/))       


      res1@cnLevels                      = xyLVs2
      res1@cnFillColors                  = xyLCs2
      res1@sfXArray                      = dlon(:, :)
      res1@sfYArray                      = dlat(:, :)

      res1@gsnLeftString                 = "e) Difference"
      res1@vpXF                          = .06
      res1@vpYF                          = .31     
    ;  plot(4) = gsn_csm_contour_map(wks, drttov(CHN, :, :), res1)
      plot(4) = gsn_csm_contour_map(wks, ddrttov, res1)

      res1@gsnLeftString                 = "f) Difference"
      res1@vpXF                          = .55
      res1@vpYF                          = .31
    ;  plot(5) = gsn_csm_contour_map(wks, dcrtm(CHN, :, :), res1)
    ;  plot(5) = gsn_csm_contour_map(wks, ddcrtm, res1)
     
      res1@lbLabelBarOn                  = True
     ; res1@cnFillDrawOrder               = "PreDraw"
     ; plot(7) = gsn_csm_contour_map(wks, dcrtm(CHN, :, :), res1)
      plot(7) = gsn_csm_contour_map(wks, ddcrtm, res1)
      x1   = 0.06
      x2   = 0.95
      y1   = 0.01
      y2   = 0.025
      labelbar_w_tri_ends(wks,plot(7), (/x1,x2/), (/y1,y2/))
      
    ;  delete(res)
      end do

       restx                    = True
       restx@txFontHeightF      = 0.014
       restx@txFontThicknessF   = 2
       restx@txFont             = _Font
       restx@txJust             = "CenterLeft"

       gsn_text_ndc (wks, "FY3C_IRAS_GLOBAL_20140220_CH20_PM", 0.30 ,0.97 ,restx)

    draw(plot)
    frame(wks)
end 
