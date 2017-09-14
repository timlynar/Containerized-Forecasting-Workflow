
;   Example script to produce plots for a WRF real-data run,
;   with the ARW coordinate dynamics option.

load "/usr/share/ncarg/nclscripts/csm/gsn_code.ncl"
load "/usr/share/ncarg/nclscripts/wrf/WRFUserARW.ncl"

begin
;
; Make a list of all files we are interested in
  FILES = systemfunc (" ls -1 " + datadir + "wrfout* ")
  numFILES = dimsizes(FILES)
  print("numFILES = " + numFILES)
  print(FILES)
  print (" ")

; We generate plots, but what kind do we prefer?
  type = "png"
  wks = gsn_open_wks(type,"plt_Surface_multi_files_mdb")

; Set some basic resources
  res = True
  res@MainTitle = "Blue Sky"
  res@Footer = False
  res@mpProjection  = "Mercator"
  res@NoHeaderFooter = True
  res@lbLabelBarOn = False
  res@gsnRightString          = ""
  res@gsnLeftString           = ""
  pltres = True
  mpres = True


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  a = addfiles(FILES+".nc","r")

  times = wrf_user_getvar(a,"times",-1)  ; get all times in the file
  ntimes = dimsizes(times)         ; number of times in the file

  mdbz = wrf_user_getvar(a,"mdbz",-1)    ; slp
  mdbz@description = "Max simulated equivalent radar reflectivity factor "

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  do it = 0,ntimes-1,1             ; TIME LOOP

    print("Working on time: " + times(it) )
    res@TimeLabel = times(it)   ; Set Valid time to use on plots

    ; Plotting options for MDBZ
      opts = res
      opts@cnFillOn = True
      opts@InitTime = True

    ; Resources to control  labelbar

      opts@lbTitleString            = "Radar reflectivity"
      opts@lbTitleDirection         = "Down"
      opts@lbTitleJust              = "CenterRight"
      opts@lbTitlePosition          = "Right"
      opts@lbTitleOffsetF           = 0.07

      opts@lbOrientation            = "Vertical"
      opts@pmLabelBarSide           = "Right"
      opts@pmLabelBarHeightF        = 0.77
      opts@pmLabelBarWidthF         = 0.11
      opts@pmLabelBarOrthogonalPosF = 0.03
      opts@lbBoxMinorExtentF        = 0.4

      contour_psl = wrf_contour(a[it],wks,mdbz(it,:,:),opts)
      delete(opts)

    ; MAKE PLOTS
      plot = wrf_map_overlays(a[it],wks,(/contour_psl/),pltres,mpres)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  end do        ; END OF TIME LOOP

end
