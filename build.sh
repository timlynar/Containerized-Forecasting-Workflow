#!/bin/bash
#     IBM Containerized Forecasting Workflow - setup script
#
#    Licensed Materials - Property of IBM
#    “Restricted Materials of IBM”
#     Copyright IBM Corp. 2017 ALL RIGHTS RESERVED

#This script downloads and compiles WPS and WRF.
#
#Will build either 3.6.1 or 3.8.1. If legacy = true then 3.6.1 else 3.9.1

legacy=false

externalsPath=/opt/deepthunder/externalDependencies

#set some environment variables
export PATH=/opt/deepthunder/externalDependencies/bin:/opt/deepthunder/externalDependencies:/usr/lib64/qt-3.3/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin::/usr/lib64/mpich/bin
export JASPERLIB=/usr/lib64
export JASPERINC=/usr/include
export DEEPTHUNDER_EXTERNALS=${externalsPath}
export CXXFLAGS="-m64 -O2 -pipe -fPIC"
export CFLAGS="${CXXFLAGS}"
export NETCDF=/usr
export NETCDF_INC=/usr/include
export NETCDF_LIB=/usr/lib64

#make the log directory
mkdir ${externalsPath}/log

#change to the src directory
cd ${externalsPath}/src

echo "Downloading source..."

if [ "$legacy" = true ] ; then
  #this is legacy 3.6.1
  #download the code
  wget http://www2.mmm.ucar.edu/wrf/src/WPSV3.6.1.TAR.gz
  wget http://www2.mmm.ucar.edu/wrf/src/WRFV3.6.1.TAR.gz

  #get the fixes for wrf.
  wget http://www2.mmm.ucar.edu/wrf/src/fix/start_em.F.fix-3.6.1
  wget http://www2.mmm.ucar.edu/wrf/src/fix/module_io_quilt.F.fix-3.6.1
  wget http://www2.mmm.ucar.edu/wrf/src/fix/mediation_feedback_domain.F.fix-3.6.1
  wget http://www2.mmm.ucar.edu/wrf/src/fix/module_first_rk_step_part1.F.fix-3.6.1

  #uncompress
  tar -xvf WRFV3.6.1.TAR.gz
  tar -xvf WPSV3.6.1.TAR.gz

  #copy over some fixes
  mv -f ${externalsPath}/src/start_em.F.fix-3.6.1 /opt/deepthunder/externalDependencies/src/WRFV3/dyn_em/start_em.F
  mv -f ${externalsPath}/src/module_io_quilt.F.fix-3.6.1 /opt/deepthunder/externalDependencies/src/WRFV3/frame/module_io_quilt.F
  mv -f ${externalsPath}/src/mediation_feedback_domain.F.fix-3.6.1 /opt/deepthunder/externalDependencies/src/WRFV3/share/mediation_feedback_domain.F
  mv -f ${externalsPath}/src/module_first_rk_step_part1.F.fix-3.6.1 /opt/deepthunder/externalDependencies/src/WRFV3/dyn_em/module_first_rk_step_part1.F

  #copy in our configuration
  cp -f ${externalsPath}/src/IBM/configure.wrf ${externalsPath}/src/WRFV3/configure.wrf
  cp -f ${externalsPath}/src/IBM/configure.wps ${externalsPath}/src/WPS/configure.wps

else
  #We want to build wrf 3.9.1 with wps 3.9.11
  #download the code
  wget http://www2.mmm.ucar.edu/wrf/src/WRFV3.9.1.1.TAR.gz
  wget http://www2.mmm.ucar.edu/wrf/src/WPSV3.9.0.1.TAR.gz

  #uncompress
  tar -xf WRFV3.9.1.1.TAR.gz
  tar -xf WPSV3.9.0.1.TAR.gz

  #copy in our configuration
  cp -f ${externalsPath}/src/IBM/configure.wrf_3.8.1 ${externalsPath}/src/WRFV3/configure.wrf
  cp -f ${externalsPath}/src/IBM/config_wps ${externalsPath}/src/WPS/configure.wps

  #change GEOGRID.TBL to use greenfrac
  sed -i 's/.*default:greenfrac_fpar_modis.*/rel_path=default:greenfrac\//' /opt/deepthunder/externalDependencies/src/WPS/geogrid/GEOGRID.TBL.ARW

  #change namelist.input num_land_cat from 24 to 21
  sed -i 's/.*num_land_cat.*/ num_land_cat	 = 21,/' /opt/deepthunder/externalDependencies/src/IBM/namelist.input

fi

#make the compile programs executable
chmod +x ${externalsPath}/src/WRFV3/compile
chmod +x ${externalsPath}/src/WPS/compile

#compile WRF
cd ${externalsPath}/src/WRFV3
echo "Building WRF in serial. Please note this will take some time ..."

./compile em_real &> ${externalsPath}/log/wrf.out

#compile WPS
cd ${externalsPath}/src/WPS
echo "Building WPS. Please note this will take some time ..."
./compile

#copy the built files
cp -r -L ${externalsPath}/src/WRFV3/run ${externalsPath}/WRF

#Touch nup.exe
touch ${externalsPath}/WRF/nup.exe

#clean up by removing wrf source code and archives.
rm -rf ${externalsPath}/src/WRFV3/
rm -f ${externalsPath}/src/*.TAR.gz

#copy over additional Vtables.
cp ${externalsPath}/src/IBM/Vtable.GFSRDA ${externalsPath}/src/WPS/ungrib/Variable_Tables/Vtable.GFSRDA
cp ${externalsPath}/src/IBM/Vtable.LIS ${externalsPath}/src/WPS/ungrib/Variable_Tables/Vtable.LIS
cp -f ${externalsPath}/src/WPS/ungrib/Variable_Tables/Vtable.GFS ${externalsPath}/src/WPS/ungrib/Variable_Tables/Vtable.GFSNEW


#download and install CDO.
echo "Downloading CDO..."
cd ${externalsPath}/src
wget https://code.mpimet.mpg.de/attachments/download/14686/cdo-1.8.2.tar.gz
tar -xf cdo-1.8.2.tar.gz
cd cdo-1.8.2
./configure --with-netcdf=/usr/lib64 --with-hdf5=/usr/lib64 --with-jasper=/usr/lib64 --with-grib_api=/usr
echo "Building CDO..."
make
make install
cd ${externalsPath}/src
rm -rf ${externalsPath}/src/cdo-1.8.1
rm ${externalsPath}/src/cdo-1.8.1.tar.gz

#download grib_api source and copy over the samples.
wget https://software.ecmwf.int/wiki/download/attachments/3473437/grib_api-1.22.0-Source.tar.gz?api=v2
tar -xf grib_api-1.22.0-Source.tar.gz?api=v2
mv grib_api-1.22.0-Source/samples /usr/share/grib_api/samples
rm -f grib_api-1.22.0-Source.tar.gz?api=v2
rm -rf grib_api-1.22.0-Source

if [ "$legacy" = true ] ; then
  #From 1200 UTC, July 19, 2017, NCEP upgraded GFS data.
  #see: http://www2.mmm.ucar.edu/wrf/users/wpsv3.9/known-prob-3.9.html
  #we need Vtable.GFS, METGRID.TBL.ARW, and to build ungrib.exe from 3.9.1 to use GFS after July 19, 2017.
  #change to the src directory
  mkdir -p ${externalsPath}/src/newWPS
  #download the required version of WPS.
  cd ${externalsPath}/src/newWPS
  echo "Downloading new ungrib..."
  wget http://www2.mmm.ucar.edu/wrf/src/WPSV3.9.0.1.TAR.gz
  tar -xvf WPSV3.9.0.1.TAR.gz
  cd ${externalsPath}/src/newWPS/WPS

  #copy over config file.
  cp -f ${externalsPath}/src/IBM/config_wps ${externalsPath}/src/newWPS/WPS/configure.wps

  #compile just ungrib.exe
  echo "Building new ungrib..."
  ./compile ungrib

  #copy Vtable.GFS, METGRID.TBL.ARW, and ungrib.exe to the WPS directory.
  cp -f ${externalsPath}/src/newWPS/WPS/ungrib/src/ungrib.exe ${externalsPath}/src/WPS/ungrib/src/ungrib.exe
  cp -f ${externalsPath}/src/newWPS/WPS/ungrib/Variable_Tables/Vtable.GFS ${externalsPath}/src/WPS/ungrib/Variable_Tables/Vtable.GFSNEW
  cp -f ${externalsPath}/src/newWPS/WPS/metgrid/METGRID.TBL.ARW ${externalsPath}/src/WPS/metgrid/METGRID.TBL.ARW

  #clean up.
  rm -f ${externalsPath}/src/newWPS/WPSV3.9.0.1.TAR.gz

fi
#unlimit the stack.
echo "ulimit -s unlimited" >> /root/.bashrc

#all done.
echo "Install complete"
