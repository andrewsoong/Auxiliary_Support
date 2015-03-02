; ===============================================
; Newstd.ncl
;
; Concepts illustrated:
;   - Filling the area between two curves in an XY plot
;   - Attaching a filled polygon to an XY plot
;   - Drawing Greek characters on an XY plot
;
; ===============================================
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"
load  "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load  "$NCARG_ROOT/lib/UDF.ncl"
;================================================
begin
;================================================
; file handling and data aquisition
;================================================
   fil_in = file_in
   f_var  = addfile(fil_in, "r") 
   Avg   = f_var->Avg
   time  = f_var->time   
    
   timlab = time(:, 0)
      
   timlab = time(:, 0)*1000000 + time(:, 1)*10000 + time(:, 2)* 100+ time(:,3) 
     
   k = 0
   k = dimsizes(timlab)
   xaix = ispan(0, k-1, 1)  
  
   indtime = ind(mod(timlab, 100) .eq. 2 )   
    
  ; xmain          = xaix(indtime) 
  ; print(xmain)  
   xmtt       = ind( mod(timlab, 200) .eq. 0 )
 ;  xmino      = xaix(xmtt)  
   caix       = sprinti("%4.4i",mod(timlab(indtime), 1000000)/100 )+"~C~"+sprinti("%4.4i", timlab(indtime)/1000000  )
         
   
;================================================
; define a polygon centered the width of 2 sigma
;================================================ 
   xp    = new( (/2*k/), integer, -9999) 
   yp    = new( (/2*k/), float )   
 do k=0,k-1
   dx = sqrt(Avg(k))  
   yp(k) = Avg(k) + dx   
   xp(k) = timlab(k)   
   xp(2*xlength-1-k) = timlab(k)   
   yp(2*xlength-1-k) = Avg(k) - dx    
 end do  
;================================================ 
; plotting parameters
;================================================
 wks   = gsn_open_wks ("png", "Std" )            ; open workstation

 res                    = True                    ; plot mods desired
 res@gsnDraw            = False                   ; don't draw
 res@gsnFrame           = False                   ; don't advance frame
 res@xyLineThicknessF   = 2.                      ; line thickness
 res@tfPolyDrawOrder    = "Predraw"               ; put line on top 
 
 ;res@tmXBValues         = xaix 
; res@tmXBLabels         = caix
; res@tmXBMinorOn        = True
; res@tmXBMinorValues    = xmino

; note that a colon is the default function code, but I have personally
; set the default code to be a tilda (~) in my .hluresfile

 res@tiMainString       = "std_test"
 plot  = gsn_csm_xy (wks,xaix,Avg,res)   ; create plot

 gsres                   = True                        ; poly res
 gsres@gsFillColor       = "SlateBlue"                 ; color chosen
dummy = gsn_add_polygon (wks,plot,xp,yp,gsres)
 
;=================================================
; add greek symbol to plot with gsn_text
;=================================================
 txres               = True                      ; text mods desired
 txres@txFontHeightF = 0.09                    ; text font height
  
 ;gsn_text(wks,plot,"S",-50,240,txres)

 draw(plot)                                            ; draw
 frame(wks)                                            ; advance frame

end