Licensed Materials - Property of IBM
 Copyright IBM Corp. 2017 All Rights Reserved.
US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

# IBM Containerized Forecasting Workflow
Contact Tim (timlynar@au.ibm.com)
Version: o1.8

## Minimum requirements
You will need a computer with docker installed, an internet connection, and about 10G of free space.

## Installation
To install:
Provided you have docker installed, go to the directory with this README.md in it and run the following:

```
docker build -t stevedore .
```


## After install but before your first run
You must make a directory on your host for storing the output data and input data.
You must install terrestrial data.


### Data directory
A data directory is needed to store the output of runs and to sore static Terrestrial data
I suggest that you run your container with /opt/deepthunder/data mounted to the host as in the below example i.e.

 ```
 -v /data:/opt/deepthunder/data
 ```
on osx or windows you will need goto docker > preferences > file sharing and share the /data folder or whatever folder you choose.

Do not forget to make the directory /data on your host i.e.
```
mkdir /data
```


### Terrestrial data
Either download your own terrestrial data and make it visible in the container at /opt/deepthunder/data/Terrestrial_Input_Data or Start the container with a mounted share:

```
docker run -w /opt/deepthunder -e DEEPTHUNDER_ROOT=/opt/deepthunder -v /data:/opt/deepthunder/data -t -i stevedore /bin/bash
```

Then execute the **download_geog_data.sh** script in the scripts sub-folder **from within the container**:

```
/bin/bash ./stevedore/scripts/download_geog_data.sh
```

This will install terrestrial data to /opt/deepthunder/data/Terrestrial_Input_Data in the container.
At the time of writing this will require about 7G of free space.



## Execution
To Execute a quick run not interactively **with a mounted data directory as defined above**:
```
docker run -w /opt/deepthunder -v /data:/opt/deepthunder/data -i stevedore /bin/python run_simulation.py --start 2017-08-12 --end 2017-08-12 --length 24 --lat 29.434 --long -98.499 --hour 00
```

Please note in the above example /opt/deepthunder/data is mounted on the host at /data.


To execute IBM Containerized Forecasting Workflow interactively try:
```
docker run -w /opt/deepthunder -e DEEPTHUNDER_ROOT=/opt/deepthunder -v /data:/opt/deepthunder/data -t -i stevedore /bin/bash
```

Then run:
```
/bin/python run_simulation.py --start 2017-08-12 --end 2017-08-12 --length 24 --lat 29.434 --long -98.499 --hour 00
```

### rda.ucar.edu (RDA)
Optionally Goto the RDA [website ](https://rda.ucar.edu/) and create for yourself a new account. This will give you access to their data. Many of the datasets require and RDA account. If you select a dataset that requires an RDA account but you do not provide any credentials, that dataset will be dropped. If you provide invalid credentials you will spend a long time waiting for the data to try to download and eventually fail.


## Execution - With RDA data.
To execute IBM Containerized Forecasting Workflow interactively try:

```
docker run -w /opt/deepthunder -e RDA_EMAIL=bruce@acompany.com -e RDA_PASS=ComPl3xpassw0rd -e DEEPTHUNDER_ROOT=/opt/deepthunder -v /data:/opt/deepthunder/data -t -i stevedore /bin/bash
```

The above command will mount data from the host directory /data to the container at /opt/deepthunder/data this is where all input and output data will be stored.


You will find yourself in the working directory. For a test run try the following quick example.

```
/bin/python run_simulation.py --ncores 16 --start 2016-04-12 --end 2016-04-12 --length 48 --lat 29.434 --long -98.499 --hour 12 --ngridew 91 --ngridns 91 --ndomains 3 --gridratio 3 --timestep 20 --gridspacinginner 2 --phys_mp 17 --phys_ralw 1 --phys_rasw 5 --phys_cu 0 --phys_pbl 1 --phys_sfcc 1 --phys_sfc 2 --phys_urb 0 --initialConditions 'GFSp25' --boundaryConditions 'GFSp25'
```


To execute a run from the host directly:
```
docker run -w /opt/deepthunder -e RDA_EMAIL=bruce@acompany.com -e RDA_PASS=ComPl3xpassw0rd -e DEEPTHUNDER_ROOT=/opt/deepthunder -v /data:/opt/deepthunder/data -i stevedore /bin/python run_simulation.py --ncores 16 --start 2016-04-12 --end 2016-04-12 --length 24 --lat 29.434 --long -98.499 --hour 12 --ngridew 91 --ngridns 91 --ndomains 3 --gridratio 3 --timestep 20 --gridspacinginner 2 --phys_mp 17 --phys_ralw 1 --phys_rasw 5 --phys_cu 0 --phys_pbl 1 --phys_sfcc 1 --phys_sfc 2 --phys_urb 0 --initialConditions 'GFSp25' --boundaryConditions 'GFSp25'
```

NOTE: as far as I know bruce@acompany.com is not a real email address. I have used it as an example. I also suggest you do not use ComPl3xpassw0rd as your password :P


## Data
The following preconfigured datasets are defined.
 - GFSFCST - 0.5 degree goes back about 2 years. Use it for forecasting.
 - GFS - at the time of writing this goes back to about 2014-09
 - GFSp25 - 0.25 degree GFS data this starts in 2015-01-15 and continues (ds084.1)
 - GFSsubset - 0.25 degree but only downloads the data for the domain not the whole world.
 - FNL -  1 degree operational Model Global Tropospheric Analyses from 1999-07-30 continuing  (ds083.2)
 - FNLp25 -  0.25 degree - Global Tropospheric Analyses and Forecast Grids from 2015-07-08 continuing (ds083.3)
 - CFSR  - From 1979 every 6 hours.  
 - CFDDA - NCAR Global Climate Four-Dimensional Data Assimilation (CFDDA) Hourly 40 km Reanalysis from 1985 - 2005 **BYO VTABLE**
 - ERAISFC - (ERA-I) is **automated data acquisition not supported in this release**
 - ERAIML - (ERA-I) is **automated data acquisition not supported in this release**
 - RAP - Rapid Refresh (RAP) Hourly from mid 2012 to current.
 - NAM - North American Mesoscale Forecast System (NAM) Hourly.
 - SSTNCEP - Sea surface Temperature dataset
 - SSTOISST - Sea surface Temperature dataset
 - SSTJPL - Sea surface Temperature dataset
 - SSTSPORT - Sea surface Temperature dataset
 - NASAGF - Green Vegetation fraction. **Data acquisition only in this release**
 - NASALISCONUS - LIS for conus only. **Data acquisition only in this release**
 - MESONET - data from madis-data.noaa.gov useful for verification inside CONUS
 - METAR - data from madis-data.noaa.gov useful for verification inside CONUS
 - PREPBufr - surface and upper air observation data in PrepBufr format.
 - LittleRSurface - surface observations in Little_R format
 - LittleRUpperAir - upper air observation in in Little_R format


## run_simulation
run_simulation.py is a  script will let you run a forecast, hindcast, or reanalysis. This script has a number of options described below:

 + --start start date and time in format yyyy-mm-dd
 + --end end date and time in format yyyy-mm-dd
 + --length forecast length in hours in format HH
 + --lat latitude in format 99.999  Can specify single value for all, or exactly one per domain starting with domain 1
 + --long longitude in format 99.999  Can specify single value for all, or exactly one per domain starting with domain 1
 + --hour start hour in format HH
 + --ngridew grid count for domains in east-west direction default = 100.  Can specify single value for all, or exactly one per domain
 + --ngridns grid count for domains in north-south direction default = 100.  Can specify single value for all, or exactly one per domain
 + --nvertlevels number of vertical levels, default = 40
 + --ndomains number of domains, default = 3
 + --gridratio nest refinement ratio for domains, default = 3
 + --gridspacinginner single value for dx and dy of inner domain, in km (others will be calculated based on ratio), default = 1.5
 + --timestep  timestep (minutes), default = 10
 + --wpsmapproj wps map projection defaults to lambert
 + --sitefile  define custom namelist for WPS and WRF
 + --tslistfile  Activate time series output. point to a tslist file see README.tslist for more info
 + --ncores define number of cores default = 2
 + --phys_mp Micro Physics scheme
 + --phys_ralw Radiation long wave physics scheme (1=RRTM, 3=CAM3, 4=RRTMG, 5=goddard, 31=Held-Suarez, 99=GFDL)
 + --phys_rasw Radiation Short wave Physics scheme (1=mm5 Dudhia, 2=Goddard, 3=cam3, 4=RRTMG, 5=New Goddard SW, 99=GFDL scheme)
 + --phys_cu Cumulus scheme (1=new Kain-Fritsch, 2=Betts-miller-Janjic, 3=Grell-Devenyi Ensemble, 4=SAS, 5=Grell-3d, 6=Tiedtke, 7=CAM Zhang-McFarlane, 14=NSAS, 99=Old Kain-Fritsch) can specify only one or exactly one for each domain
 + --phys_pbl Planetary boundary layer (1=YSU , 2=Mellor-Yamada-janjic , 3=Eta, 4=QNSE, 5=MYNN, 6=MYNN, 7=ACM2, 8=Boulac , 9=CAM UW , 10=TEMF, 99=MRF)
 + --phys_sfcc Surface clay physics
 + --phys_sfc Surface physics
 + --phys_urb Urban physics
 + --runshort number of extra hours to prep but not execute
 + --auxhist7 turn on auxhist7 output to hourly
 + --auxhist2 turn on auxhist2 output to hourly
 + --feedback Turn feedback on
 + --adaptivets turn adaptive time steps on
 + --no-preprocess turn pre-processing off
 + --projectdir the project sub-directory. i.e. what do you want this project called.
 + --no-wrf turn wrf.exe execution off
 + --is-Analysis perform a historical simulation
 + --altftpserver if this is set then all data will be downloaded from this ftp server rather than those preset for each dataset.
 + --initialConditions data to use for initial conditions
 + --boundaryConditions data to use for boundary conditions
 + --inputData input data for IC and LBCs
 + --history_interval history output file interval in minutes, default = 60

***

## Terrestrial data usage
namelist.wps has a line to specify the sort of static terrestrial data to use this line is not altered by the provided workflow.
If you do not want the default terrestrial data selected then please alter the namelist.wps template file in
`/opt/deepthunder/externalDependencies/namelist.wps` The line is `geog_data_res  = 'default', 'default', 'default', 'default',`
see [Chapter 3 of the WRF user guide ](http://www2.mmm.ucar.edu/wrf/users/docs/user_guide_V3/users_guide_chap3.htm) for more information.


## Post processing and visualisation
BYO Post processing and visualisation.

Note that Post processing, visualization, verification, FDDA, and 3dvar have been striped from this version.


## Known Issues
Send me an email when you find one and I will populate this list.
1. --sitefile does not do anything.
2. If the user does not have terrestrial input data, the workflow will try to download and extract it for them, if this happens you may run out of disk space.
3. CFDDA,  NASAGF and NASALISCONUS are supported for data acquisition only.
4. if you run into segfaults when running WRF consider increasing the limits on your stack (ulimit -s unlimited)

# Execution of unit-tests
To run unit tests on input data sources try the following command:
`python -m stevedore.test.test-inputdatasets`


## Alpha notice
This is an alpha release. There are limitations to the testing performed and bugs may pop up. At this stage I have not tested many of the possible data source combinations. These may contain bugs, or just not work at all.
