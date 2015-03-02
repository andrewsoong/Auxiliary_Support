; ===============================================
; std.ncl
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
   Avg    = f_var->Avg
   dtime  = f_var->time   
   nnn    = 0
   nnn    = dimsizes(dtime(:,0)) 
   ;xaix            = ispan(0, nnn-1, 1)  
   
   timlab = dtime(:, 0)
   timlab = dtime(:, 0)*1000000 + dtime(:, 1)*10000 + dtime(:, 2)* 100+ dtime(:,3)
  
   indtime = ind(mod(timlab, 100) .eq. 2 )   
   ntime = xaix(indtime)
   indAvg = ind(Avg)  
   nAvg = xaix(indAvg)   
   xlength = dimsizes(ntime)  
;================================================
; define a polygon centered the width of 2 sigma
;================================================ 
   xp    = new( (/2*xlength/), integer ) 
   yp    = new( (/2*xlength/), float )   
 do k=0,xlength-1
   dx = sqrt(nAvg(k))  
   yp(k) = nAvg(k) + dx   
   xp(k) = ntime(k)   
   xp(2*xlength-1-k) = ntime(k)   
   yp(2*xlength-1-k) = nAvg(k) - dx    
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

; note that a colon is the default function code, but I have personally
; set the default code to be a tilda (~) in my .hluresfile

 res@tiMainString       = "std_test"
 plot  = gsn_csm_xy (wks,ntime,nAvg,res)   ; create plot

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