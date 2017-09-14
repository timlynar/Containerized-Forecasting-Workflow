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
    “Restricted Materials of IBM”
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

class InputDataSetMESONET(InputDataSet):
    '''
    A class defining a MADIS (MESONET) input data set file in netcdf.
    '''
    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):

        InputDataSet.__init__(self, date, hour, path, **args)
        # Add hour_step to date (should be hourly)
        date_delta = self.date+timedelta(self.hour/24.)
        self.is_rda = [False]
        self.type = 'MESONET'
        self.path = path+'/MESONET'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.server_url = ['ftp://pftp.madis-data.noaa.gov']
        self.ungrib_prefix = None
        self.ungrib = False
        self.server_path = ['archive/'+str(date_delta.year)+str(date_delta.month).zfill(2)+\
                            str(date_delta.day).zfill(2)+'/LDAD/mesonet/netCDF/']


    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''

        date_delta = self.date+timedelta(self.hour/24.)
        return str(date_delta.year)+str(date_delta.month).zfill(2)+str(date_delta.day).zfill(2)+\
               '_'+str(date_delta.hour).zfill(2)+'00.gz'


class InputDataSetMETAR(InputDataSet):
    '''
    A class defining a MADIS (METAR) input data set file in netcdf.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):

        InputDataSet.__init__(self, date, hour, path, **args)
        # Add hour_step to date (should be hourly)
        date_delta = self.date+timedelta(self.hour/24.)
        self.type = 'METAR'
        self.path = path+'/METAR'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.server_url = ['ftp://madis-data.ncep.noaa.gov']
        self.ungrib = False
        self.ungrib_prefix = None
        self.server_path = ['archive/'+str(date_delta.year)+'/'+str(date_delta.month).zfill(2)+\
                           '/'+str(date_delta.day).zfill(2)+'/point/metar/netcdf/']
        self.is_rda = [False]


    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''

        date_delta = self.date+timedelta(self.hour/24.)
        return str(date_delta.year)+str(date_delta.month).zfill(2)+\
               str(date_delta.day).zfill(2)+'_'+str(date_delta.hour).zfill(2)+'00.gz'

    #prepare is never called so we can ditch this.
    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''
        os.chdir(self.path)
        for filename in glob.glob('*.gz'):

            #find the extracted name
            ename = filename[:-3]

            #gunzip it and move it.
            process = subprocess.Popen(['gunzip', filename])
            process.wait()
            logging.info('preparing METAR data')

            #make self.directory_root_observations+'/MADIS/
            pdir = self.DIRECTORY_ROOT_OBSERVATIONS+'/MADIS/'
            try:
                os.stat(pdir)
            except:
                os.mkdir(pdir)
                logging.info('making MADIS dir')


            #Move the file now to this directory.
            logging.info('preparing METAR data moving '+ ename +' to '+ pdir +ename)
            shutil.move(ename, pdir +ename)


class InputDataSetPREPBufr(InputDataSet):
    '''
    A class defining a GDAS-Prepbufr input data set file in netcdf.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):

        InputDataSet.__init__(self, date, hour, path, **args)
        # Add hour_step to date (should be hourly)
        date_delta = self.date+timedelta(self.hour/24)
        self.is_rda = [False]
        self.type = 'PREPBUFR'
        self.path = path+'/PREPBUFR'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.server_url = ['http://rda.ucar.edu']
        self.server_path = ['data/ds337.0/tarfiles/'+str(date_delta.year)+'/'+self.get_filename()]
        self.ungrib = False
        self.ungrib_prefix = None
        self.is_rda = True

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''

        return 'prepbufr.'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+'.nr.tar.gz'

    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''
        return 'prepbufr.'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+'.nr'

    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''
        logging.info('Extracting Prepbufr...')
        os.chdir(self.path)
        for filename in glob.glob('*.gz'):
            process = subprocess.Popen(['tar', '-zxvf', filename])
            process.wait()


class InputDataSetLittleRSurface(InputDataSet):
    '''
    little_R formatted NCEP ADP Global Surface Observational Weather Data,
    October 1999 - continuing
    ds461.0 - used for nudging
    e.g. https://rda.ucar.edu/data/ds461.0/little_r/2017/SURFACE_OBS:2017010100
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):

        InputDataSet.__init__(self, date, hour, path, **args)
        # Add hour_step to date (should be hourly)
        self.lrdate_delta = self.date+timedelta(hours=self.hour)
        self.keep_existing_file = True
        self.type = 'LITTLE_R'
        self.path = path+'/LITTLE_R'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.server_url = ['http://rda.ucar.edu']
        self.server_path = ['data/ds461.0/little_r/'+str(self.lrdate_delta.year)]
        self.ungrib = False
        self.ungrib_prefix = None
        self.is_rda = [True]

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'SURFACE_OBS:'+str(self.lrdate_delta.year)+str(self.lrdate_delta.month).zfill(2)+\
                str(self.lrdate_delta.day).zfill(2)+str(self.lrdate_delta.hour).zfill(2)

    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''
        return 'SURFACE_OBS:'+str(self.lrdate_delta.year)+str(self.lrdate_delta.month).zfill(2)+\
                str(self.lrdate_delta.day).zfill(2)+str(self.lrdate_delta.hour).zfill(2)



class InputDataSetLittleRUpperAir(InputDataSet):
    '''
    little_R formatted NCEP ADP Global Upper Air Observational Weather Data,
    October 1999 - continuing
    ds351.0 - used for nudging
    e.g. https://rda.ucar.edu/data/ds351.0/little_r/2017/OBS:2017010318
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):

        InputDataSet.__init__(self, date, hour, path, **args)
        # Add hour_step to date (should be hourly)
        self.lrdate_delta = self.date+timedelta(hours=self.hour)
        self.keep_existing_file = True
        self.type = 'LITTLE_R'
        self.path = path+'/LITTLE_R'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.server_url = ['http://rda.ucar.edu']
        self.server_path = ['data/ds351.0/little_r/'+str(self.lrdate_delta.year)]
        self.ungrib = False
        self.ungrib_prefix = None
        self.is_rda = [True]

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'OBS:'+str(self.lrdate_delta.year)+str(self.lrdate_delta.month).zfill(2)+\
                str(self.lrdate_delta.day).zfill(2)+str(self.lrdate_delta.hour).zfill(2)

    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''
        return 'OBS:'+str(self.lrdate_delta.year)+str(self.lrdate_delta.month).zfill(2)+\
                str(self.lrdate_delta.day).zfill(2)+str(self.lrdate_delta.hour).zfill(2)


class InputDataSetNASALISCONUS(InputDataSet):
    '''
    A class defining a NASA SPoRT LIS 3-km data (CONUS) data set file.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'SST-NASALISCONUS'
        self.path = path+'/sportlis_3km'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.server_url = ['ftp://geo.msfc.nasa.gov']
        self.server_path = ['SPoRT/modeling/lis/conus3km']
        self.ungrib_prefix = 'NASALISCONUS'
        self.is_rda = [False]
        self.intervalseconds = 3600   #Every hour


    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''

        return 'sportlis_conus3km_model_'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+'_'+str(self.date.hour).zfill(2)+'00.grb2'


class InputDataSetNASAGF(InputDataSet):
    '''
     NASA VIIRS Green Vegetation Fraction (NASA-GFV), 4k res.
     Instructions on use:
     ftp://geo.msfc.nasa.gov/SPoRT/modeling/viirsgvf/
     VIIRSGVF_Instructions/VIIRSGVF_UEMS_Instructions_v2.pdf
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):

        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.intervalseconds = 86400
        self.type = 'SST-NASAGVF'
        self.path = path+'/NASAGVF'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.server_url = ['ftp://geo.msfc.nasa.gov']
        self.ungrib = False
        self.ungrib_prefix = None
        self.server_path = ['SPoRT/modeling/viirsgvf/global']
        self.is_rda = [False]


    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''

        return '00001-10000.00001-05000.'+str(self.date.year)+\
                str(self.date.month).zfill(2)+str(self.date.day).zfill(2) + '.bz2'

    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''
        return '00001-10000.00001-05000.'+str(self.date.year)+\
                str(self.date.month).zfill(2)+str(self.date.day).zfill(2)

    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''
        os.chdir(self.path)
        for filename in glob.glob('*.bz2'):
            process = subprocess.Popen(['bunzip2', filename])
            process.wait()
