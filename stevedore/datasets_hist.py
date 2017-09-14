#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
IBM Containerized Forecasting Workflow

DESCRIPTION

    This file contains the class representing the different sources of input data sets.

AUTHOR

    Timothy Lynar <timlynar@au1.ibm.com>, IBM Research, Melbourne, Australia
    Frank Suits <frankst@au1.ibm.com>, IBM Research, Melbourne, Australia;
                                       Dublin, Ireland; Yorktown, USA
    Beat Buesser <beat.buesser@ie.ibm.com>, IBM Research, Dublin, Ireland

NOTICE

    Licensed Materials - Property of IBM
    "Restricted Materials of IBM"
     Copyright IBM Corp. 2017 ALL RIGHTS RESERVED
    US GOVERNMENT USERS RESTRICTED RIGHTS - USE, DUPLICATION OR DISCLOSURE
    RESTRICTED BY GSA ADP SCHEDULE CONTRACT WITH IBM CORP.
    THE SOURCE CODE FOR THIS PROGRAM IS NOT PUBLISHED OR OTHERWISE DIVESTED OF
    ITS TRADE SECRETS, IRRESPECTIVE OF WHAT HAS BEEN DEPOSITED WITH
    THE U. S. COPYRIGHT OFFICE. IBM GRANTS LIMITED PERMISSION TO LICENSEES TO
    MAKE HARDCOPY OR OTHER REPRODUCTIONS OF ANY MACHINE- READABLE DOCUMENTATION,
    PROVIDED THAT EACH SUCH REPRODUCTION SHALL CARRY THE IBM COPYRIGHT NOTICES
    AND THAT USE OF THE REPRODUCTION SHALL BE GOVERNED BY THE TERMS AND
    CONDITIONS SPECIFIED BY IBM IN THE LICENSED PROGRAM SPECIFICATIONS. ANY
    REPRODUCTION OR USE BEYOND THE LIMITED PERMISSION GRANTED HEREIN SHALL BE A
    BREACH OF THE LICENSE AGREEMENT AND AN INFRINGEMENT OF THE APPLICABLE
    COPYRIGHTS.

"""

import os
import logging
import ftplib
from multiprocessing import current_process
import time
from datetime import datetime, timedelta
import subprocess
import shutil
import glob
from inputdataset import *

class InputDataSetGFS(InputDataSet):
    '''
    Global Forecast System (GFS) input data set file in grib format.
    '''
    # pylint: disable=too-many-instance-attributes

    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.type = 'GFS'
        self.intervalseconds = 10800
        self.path = path+'/GFS'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.server_url = ['ftp://nomads.ncdc.noaa.gov']
        self.server_path = ['GFS/Grid4/'+str(self.date.year)+str(self.date.month).zfill(2)+\
                           '/'+str(self.date.year)+str(self.date.month).zfill(2)+\
                           str(self.date.day).zfill(2)]

        self.is_rda = [False]
        self.ungrib_prefix = 'GFS'

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'gfs_4_'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+'_'+str(self.date.hour).zfill(2)+\
                '00_'+str(self.hour).zfill(3)+'.grb2'


class InputDataSetGFSp25(InputDataSet):
    '''
    Global Forecast System (GFS) input data set file in grib format.
    0.25 degree gfs data ds084.1
    '''
    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.type = 'GFSp25'
        self.intervalseconds = 10800
        self.path = path+'/GFS'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.keep_existing_file = True
        self.ungrib = True
        self.ungrib_prefix = 'GFSRDA'
        self.server_url = ['http://rda.ucar.edu']
        self.server_path = ['data/ds084.1/'+str(self.date.year)+'/'+\
                           str(self.date.year)+str(self.date.month).zfill(2)+\
                           str(self.date.day).zfill(2)]
        self.is_rda = [True]

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'gfs.0p25.'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+str(self.date.hour).zfill(2)+'.f'+\
                str(self.hour).zfill(3)+'.grib2'




class InputDataSetFNL(InputDataSet):
    '''
    A class defining NCEP FNL Operational Model Global Tropospheric Analyses,
    continuing from July 1999
    To use this class you must specify your UCAR username (email address) and password
    via the environment variables RDA_EMAIL and RDA_PASS
    The FNL data is 1 degree unless it is after 2015-07-08 at which point
    you can get 0.25 degree data.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        #So basically we need to increment the day by hours if this is used for analysis.
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'FNL'
        self.path = path+'/FNL'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.keep_existing_file = True
        self.intervalseconds = 21600
        self.server_url = ['http://rda.ucar.edu']
        self.server_path = ['data/ds083.2/grib2/'+str(self.date.year)+'/'+\
                            str(self.date.year)+'.'+str(self.date.month).zfill(2)]
        self.ungrib_prefix = 'FNL'
        self.is_rda = [True]

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'fnl_'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+'_'+str(self.date.hour).zfill(2)+'_00.grib2'


class InputDataSetFNLp25(InputDataSet):
    '''
    NCEP FNL Operational Model Global Tropospheric Analyses, continuing from July 1999
    To use this class you must specify your UCAR username (email address)
    and password via the environment variables RDA_EMAIL and RDA_PASS
    The FNL data is 1 degree unless it is after 2015-07-08 at which point you
    can get 0.25 degree data.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'FNLp25'
        self.path = path+'/FNL'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.keep_existing_file = True
        self.intervalseconds = 21600
        self.server_url = ['http://rda.ucar.edu']
        self.server_path = ['data/ds083.3/'+str(self.date.year)+'/'+\
                            str(self.date.year)+str(self.date.month).zfill(2)]
        self.ungrib_prefix = 'FNL'
        self.is_rda = [True]

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        #Using f00 for each file might not be what you were expecting. Edit if needed.
        return 'gdas1.fnl0p25.'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+str(self.date.hour).zfill(2)+'.f00.grib2'

class InputDataSetCFSR(InputDataSet):
    '''
    Climate Forecast System Reanalysis (CFSR)
    http://soostrc.comet.ucar.edu/data/grib/cfsr/
    See: https://climatedataguide.ucar.edu/climate-data/climate-forecast-system-reanalysis-cfsr
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'CFSR'
        self.path = path+'/CFSR'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.keep_existing_file = True
        self.intervalseconds = 21600
        self.server_url = ['http://soostrc.comet.ucar.edu']
        self.server_path = ['data/grib/cfsr/'+str(self.date.year)+'/'+str(self.date.month).zfill(2)]
        self.ungrib_prefix = 'CFSR'
        self.is_rda = [False]

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        outname = ''
        if self.date.date() > datetime(2011, 04, 01).date():
            outname = str(self.date.year)[2:]+str(self.date.month).zfill(2)+\
                      str(self.date.day).zfill(2)+str(self.date.hour).zfill(2)+\
                      '.cfsrr.t'+str(self.date.hour).zfill(2)+'z.pgrb2f00'
        else:
            outname = 'pgbh00.cfsr.'+str(self.date.year)+str(self.date.month).zfill(2)+\
                       str(self.date.day).zfill(2)+str(self.date.hour).zfill(2)+'.grb2'
        return outname


class InputDataSetCFDDA(InputDataSet):
    '''
    NCAR Global Climate Four-Dimensional Data Assimilation (CFDDA) Hourly 40 km
    Reanalysis dataset is a dynamically-downscaled dataset with
    high temporal and spatial resolution that was created using NCAR's CFDDA system.
    see: https://rda.ucar.edu/datasets/ds604.0/
    This dataset contains hourly analyses with 28 vertical levels on a 40 km
    horizontal grid (0.4 degree grid increment)
    1985 to 2005
    top hpa = 0.998
    Documentation for this dataset can be found here:
    https://rda.ucar.edu/datasets/ds604.0/docs/CFDDA_User_Documentation_Rev3.pdf
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'CFDDA'
        self.path = path+'/CFDDA'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.keep_existing_file = True
        self.intervalseconds = 3600
        self.server_url = ['http://rda.ucar.edu']
        self.server_path = ['data/ds604.0/'+str(self.date.year)+'/'+str(self.date.month).zfill(2)]
        self.ungrib_prefix = 'CFDDA'
        self.is_rda = [True]

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'cfdda_'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+str(self.date.hour).zfill(2)+'.v2.nc'

    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''
        return self.get_filename()+'.grb1'

    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''
        logging.info('WPS: Converting netCDF to GRIB1 file for WPS')
        try:
            os.chdir(self.path)
            for filename in glob.glob('*.nc'):
                process = subprocess.Popen(['ncks', '-3', filename, 'temp.nc'])
                process.wait()
                process = subprocess.Popen(['cdo', '-a', '-f', 'grb1', 'copy',
                                            'temp.nc', filename+'.grb1'])
                process.wait()
                os.remove('temp.nc')
        except:
            logging.warning('WPS: Converting netCDF to GRIB1 file for WPS Failed')




class InputDataSetERAISFC(InputDataSet):
    '''
    ECWMF Reanalysis Interim (ERA-I) grib file sfc files
    NOTE: You need to download the data to your own server then edit this entry.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'ERAI'
        self.path = path+'/ERAI'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.keep_existing_file = True
	    #You need to download the data and put it on an ftp server yourself.
        self.server_url = ['ftp://10.118.50.245']
        self.server_path = ['/pub']
        self.is_rda = [False]
        self.ungrib_prefix = 'ERAI'

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''

        return 'ERA-Int_sfc_'+str(self.date.year)+str(self.date.month).zfill(2)+'01.grb'



class InputDataSetERAIML(InputDataSet):
    '''
    ECWMF Reanalysis Interim (ERA-I) grib file files Model level data.
    NOTE: You need to download the data to your own server then edit this entry.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'ERAI'
        self.path = path+'/ERAI'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.keep_existing_file = True
        #You need to download the data and put it on an ftp server yourself.
        self.server_url = ['ftp://10.118.50.245']
        self.server_path = ['/pub']
        self.is_rda = [False]
        self.ungrib_prefix = 'ERAI'

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'ERA-Int_ml_'+str(self.date.year)+str(self.date.month).zfill(2)+'01.grb'
