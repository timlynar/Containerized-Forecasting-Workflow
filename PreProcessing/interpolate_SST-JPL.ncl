;
;    Licensed Materials - Property of IBM
;    “Restricted Materials of IBM”
;     Copyright IBM Corp. 2017 ALL RIGHTS RESERVED

load "/usr/share/ncarg/nclscripts/csm/gsn_code.ncl"
load "/usr/share/ncarg/nclscripts/wrf/WRFUserARW.ncl"
load "/usr/share/ncarg/nclscripts/csm/gsn_csm.ncl"
load "/usr/share/ncarg/nclscripts/csm/contributed.ncl"
load "/usr/share/ncarg/nclscripts/wrf/WRF_contributed.ncl"

begin

f = addfile("sst.nc","r")
sst = f->analysed_sst
guess = 1
is_cyclic = False
ncscan = 500
eps = 1.e-2
relc = 0.6
opt = 0

poisson_grid_fill(sst,is_cyclic,guess,ncscan,eps,relc,opt)

ncout = "interp.nc"
system("rm -rf "+ncout)
b = addfile(ncout,"c")
b@units = "Kelvin"
b@long_name = "SST Temperatures - with Interpolation"
filedimdef(b,"Time",-1,True)

b->SST=sst

end
