#!/bin/bash
#
#    Licensed Materials - Property of IBM
#    “Restricted Materials of IBM”
#     Copyright IBM Corp. 2017 ALL RIGHTS RESERVED

export "WRF_DIR=$PWD/externalDependencies/src/WRFV3"
export "WPS_DIR=$PWD/externalDependencies/src/WPS"
export "HDF5_DIR=/usr/lib64"
export "LD_LIBRARY_PATH=$PWD/externalDependencies/lib:/usr/local/lib:/usr/lib:/usr/lib64:/usr/lib64/mpich/lib"
export "CXXFLAGS=-m64 -O2 -pipe -fPIC"
export "CPPFLAGS=-I$PWD/externalDependencies/include -I/usr/include"
export "MFLAGS=-j 4"
export "PATH=$PWD/externalDependencies/bin:$PWD/externalDependencies:/usr/lib64/qt-3.3/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin:/usr/lib64/openmpi/bin:/usr/lib64/mpich/bin"
export "LD_RUN_PATH=$PWD/externalDependencies/lib:/usr/local/lib:/usr/lib:/usr/lib64:/usr/lib64/mpich/lib"
export "CFLAGS=-m64 -O2 -pipe -fPIC"
export "NETCDF=/usr"
export "NETCDF_INC=/usr/include"
export "NETCDF_LIB=/usr/lib64"
export "DEEPTHUNDER_ROOT=$PWD"
export "DEEPTHUNDER_EXTERNALS=$PWD/externalDependencies"
export "NCARG_ROOT=/usr"
export "DEEPTHUNDER_ROOT=$PWD"
export "WRFIO_NCD_LARGE_FILE_SUPPORT=1"
export "F77=gfortran"
export "FFLAGS=-m64"
export "FC=gfortran"
ulimit -s unlimited

cd /opt/deepthunder
