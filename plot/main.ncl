;*************************************************
; histo_mwts.eps
; Chreated by Qifeng Lu Initially at ECMWF in 2012 to plot the geo map with the histogramed legend
; Modified by Qifeng Lu to fit the subplot in 2014 
;
; Concepts illustrated:
;   - Drawing a histogram legend using exact min/max of data
;
;************************************************
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"   
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load "./UDF.ncl"
;************************************************
begin

; Read data into a nrows x ncols float array.
  field   = addfile("FY3C_MWTS_GLOBAL_20141010_CH01_AM.HDF", "r")
 
  lon     = field->lon ;(:1000)
  lat     = field->lat ;(:1000)
  R       = field->diff_crtm ;(:1000)
  
  lonb     = ndtooned(lon)
  latb     = ndtooned(lat)
  Rb       = ndtooned(R)
 
  wtype   = "png"
  wks     = gsn_open_wks(wtype,"histo_mwts")
;  drawNDCGrid(wks)

;;
;;;; compulsive resource elements
;;  res@nplot    = 7    ; number of sub-plot
;;  res@iplot    = 1    ; index of sub-plot

;;;; optional resource elements
;;;;;; Region in plots
;;  res@mpMinLatF   = -90
;;  res@mpMaxLatF   = 90
;;  res@mpMinLonF   = 0
;;  res@mpMaxLonF   = 360

;;;;;; Valid data range.DEFAULT: CALCULATED based on 99% data which is retained.
;;  res@ValRange1 ;; minimal
;;  res@ValRange2 ;; maximal
;;  res@DataRetain   

;;  res@gsnLeftString    ;; String 
 
  res        = True
  res@nplot  = 6
;  res@ValRange1  = -1.43
;  res@ValRange2  = 1.5
;  res@gsnPaperOrientation = "LandScape"
  res@mpCenterLonF = 120.0
 
  res@nxxx   = 2
  res@nyyy   = 3
;  do i = 0, res@nplot - 1
    res@iplot  = 5
    res@ixxx   = 0
    res@iyyy   = 2
    iout    = histo4_1d( wks, Rb, latb, lonb, res )
;  end do
 
  frame(wks)
end



