#!/bin/bash
#
#Licensed Materials - Property of IBM
#  "Restricted Materials of IBM"
#   Copyright IBM Corp. 2017 ALL RIGHTS RESERVED
#  US GOVERNMENT USERS RESTRICTED RIGHTS - USE, DUPLICATION OR DISCLOSURE
#  RESTRICTED BY GSA ADP SCHEDULE CONTRACT WITH IBM CORP.
#  THE SOURCE CODE FOR THIS PROGRAM IS NOT PUBLISHED OR OTHERWISE DIVESTED OF
#  ITS TRADE SECRETS, IRRESPECTIVE OF WHAT HAS BEEN DEPOSITED WITH
#  THE U. S. COPYRIGHT OFFICE. IBM GRANTS LIMITED PERMISSION TO LICENSEES TO
#  MAKE HARDCOPY OR OTHER REPRODUCTIONS OF ANY MACHINE- READABLE DOCUMENTATION,
#  PROVIDED THAT EACH SUCH REPRODUCTION SHALL CARRY THE IBM COPYRIGHT NOTICES
#  AND THAT USE OF THE REPRODUCTION SHALL BE GOVERNED BY THE TERMS AND
#  CONDITIONS SPECIFIED BY IBM IN THE LICENSED PROGRAM SPECIFICATIONS. ANY
#  REPRODUCTION OR USE BEYOND THE LIMITED PERMISSION GRANTED HEREIN SHALL BE A
#  BREACH OF THE LICENSE AGREEMENT AND AN INFRINGEMENT OF THE APPLICABLE
#  COPYRIGHTS.
#
#
# This script will download static data to /opt/deepthunder/data/Terrestrial_Input_Data
# If you are running this in a container I would suggest that you mount /opt/deepthunder/data/ to a directory on the host.
#
# This script only downlads the data you need for 'default' to work in namelist.wps .
# There is more data to choose from see: http://www2.mmm.ucar.edu/wrf/users/download/get_sources_wps_geog.html
# If legacy = true then the data required for wps 3.6.1 will be downloaded else the data for wps 3.9 will be downloaded.
#

legacy=false

mkdir -p /opt/deepthunder/data/Terrestrial_Input_Data
cd /opt/deepthunder/data/Terrestrial_Input_Data

wget http://www2.mmm.ucar.edu/wrf/src/wps_files/geog_minimum.tar.bz2
wget http://www2.mmm.ucar.edu/wrf/src/wps_files/orogwd_10m.tar.bz2
wget http://www2.mmm.ucar.edu/wrf/src/wps_files/topo_2m.tar.bz2
wget http://www2.mmm.ucar.edu/wrf/src/wps_files/landuse_2m.tar.bz2
wget http://www2.mmm.ucar.edu/wrf/src/wps_files/soiltype_top_2m.tar.bz2
wget http://www2.mmm.ucar.edu/wrf/src/wps_files/soiltype_bot_2m.tar.bz2

tar -xvf geog_minimum.tar.bz2
tar -xvf orogwd_10m.tar.bz2
tar -xvf topo_2m.tar.bz2
tar -xvf landuse_2m.tar.bz2
tar -xvf soiltype_top_2m.tar.bz2
tar -xvf soiltype_bot_2m.tar.bz2

rm geog_minimum.tar.bz2
rm orogwd_10m.tar.bz2
rm topo_2m.tar.bz2
rm landuse_2m.tar.bz2
rm soiltype_top_2m.tar.bz2
rm soiltype_bot_2m.tar.bz2




if [ "$legacy" = true ] ; then
  #download datasets that are not needed in 3.9.1
  echo "downloading of Terrestrial Input Data needed for wps 3.6.1 complete."


else
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/topo_gmted2010_30s.tar.bz2
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/modis_landuse_20class_30s_with_lakes.tar.bz2
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/soiltype_top_30s.tar.bz2
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/soiltype_bot_30s.tar.bz2
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/NUDAPT44_1km.tar.bz2
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/nlcd2011_imp_ll_9s.tar.bz2
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/nlcd2011_can_ll_9s.tar.bz2
  wget http://www2.mmm.ucar.edu/wrf/src/wps_files/greenfrac.tar.bz2

  tar -xvf topo_gmted2010_30s.tar.bz2
  tar -xvf modis_landuse_20class_30s_with_lakes.tar.bz2
  tar -xvf soiltype_top_30s.tar.bz2
  tar -xvf soiltype_bot_30s.tar.bz2
  tar -xvf NUDAPT44_1km.tar.bz2
  tar -xvf nlcd2011_imp_ll_9s.tar.bz2
  tar -xvf nlcd2011_can_ll_9s.tar.bz2
  tar -xvf greenfrac.tar.bz2

  rm topo_gmted2010_30s.tar.bz2
  rm modis_landuse_20class_30s_with_lakes.tar.bz2
  rm soiltype_top_30s.tar.bz2
  rm soiltype_bot_30s.tar.bz2
  rm NUDAPT44_1km.tar.bz2
  rm nlcd2011_imp_ll_9s.tar.bz2
  rm nlcd2011_can_ll_9s.tar.bz2
  rm greenfrac.tar.bz2

  echo "downloading of Terrestrial Input Data needed for wps 3.9 complete."

fi
