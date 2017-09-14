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

class InputDataSetGFSFCST(InputDataSet):
    '''
    Global Forecast System (GFS) input data set file in grib format.
    '''
    # pylint: disable=too-many-instance-attributes

    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.type = 'GFSFCST'
        self.hour = hour
        self.intervalseconds = 10800
        self.path = path+'/GFSFCST'
        self.name = self.get_filename()
        self.keep_existing_file = True
        self.name_prepared = self.get_filename()
        self.server_url = ['http://soostrc.comet.ucar.edu']
        self.server_path = ['data/grib/gfs/'+str(self.date.year)+\
                            str(self.date.month).zfill(2)+str(self.date.day).zfill(2)+\
                            '/grib.t'+str(self.date.hour).zfill(2)+'z']

        self.is_rda = [False]
        self.ungrib_prefix = 'GFS'

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        17062718.gfs.t18z.0p50.pgrb2f036
        '''
        return str(self.date.year)[2:]+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+str(self.date.hour).zfill(2)+\
                '.gfs.t'+str(self.date.hour).zfill(2)+'z.0p50.pgrb2f'+str(self.hour).zfill(3)


class InputDataSetGFSsubset(InputDataSet):
    '''
    Global Forecast System (GFS) input data set file with 0.25 degree resolution
    in grib format for the upgraded version after January 15, 2015.
    This only has data about 12 days old. It is hourly
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        if args:
            lon_min = args['lon_min']
            lon_max = args['lon_max']
            lat_min = args['lat_min']
            lat_max = args['lat_max']

        self.type = 'GFSsubset'
        self.path = path+'/GFSsubset'
        self.name = 'filter_gfs_0p25.pl?file=gfs.t'+str(self.date.hour).zfill(2)+\
                    'z.pgrb2.0p25.f'+str(self.hour).zfill(3)+\
                    '&all_lev=on&all_var=on&subregion=&leftlon='+str(lon_min)+\
                    '&rightlon='+str(lon_max)+'&toplat='+str(lat_max)+\
                    '&bottomlat='+str(lat_min)+'&dir=%2Fgfs.'+str(self.date.year)+\
                    str(self.date.month).zfill(2)+str(self.date.day).zfill(2)+\
                    str(self.date.hour).zfill(2)

        self.name_prepared = self.get_filename()
        self.keep_existing_file = False
        self.ungrib = True
        self.server_url = ['http://nomads.ncep.noaa.gov']
        self.server_path = ['cgi-bin']
        self.ungrib_prefix = 'GFSsubset'
        self.is_rda = [False]
        self.intervalseconds = 3600


    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return 'gfs.t'+str(self.date.hour).zfill(2)+'z.pgrb2.0p25.f'+str(self.hour).zfill(3)


    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''
        try:
            os.chdir(self.path)
            for filename in glob.glob('filter_gfs_0p25.pl*'):
                logging.info('WPS: Renaming '+filename)
                pfilename = filename[24:48]
                os.rename(filename, pfilename)

        except:
            logging.warning('GFSsubset prepare failure')



class InputDataSetRAP(InputDataSet):
    '''
    Rapid Refresh (RAP) input data set file.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.date = self.date + timedelta(hours=self.hour)
        self.type = 'RAP'
        self.path = path+'/RAP'
        self.intervalseconds = 3600 #every hour
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.is_rda = [False]
        self.ungrib_prefix = 'RAP'
        if self.date.date() >= datetime.today().date() - timedelta(days=2):
            self.keep_existing_file = False
            self.server_url = ['ftp://ftp.ncep.noaa.gov']
            self.server_path = ['pub/data/nccf/com/rap/prod']
        else:
            self.keep_existing_file = True
            self.server_url = ['http://soostrc.comet.ucar.edu']
            self.server_path = ['data/grib/rap/'+str(self.date.year)+\
                                str(self.date.month).zfill(2)+\
                                str(self.date.day).zfill(2)+'/hybrid']

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        fileout = ''

        if self.date.date() >= datetime.today().date()  - timedelta(days=2):
            fileout = 'rap.t'+str(self.date.hour).zfill(2)+'z.awp130bgrbf'+\
                       str(self.hour).zfill(2)+'.grib2'
        else:
            fileout = str(self.date.year)[2:4]+str(self.date.month).zfill(2)+\
                      str(self.date.day).zfill(2)+str(self.date.hour).zfill(2)+\
                      '.rap.t'+str(self.date.hour).zfill(2)+'z.awp130bgrbf00.grib2'

        return fileout



class InputDataSetNAM(InputDataSet):
    '''
    North American Mesoscale Forecast System (NAM) input data set file.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.intervalseconds = 3600   #Every hour
        self.type = 'NAM'
        self.path = path+'/NAM'
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        self.is_rda = [False]

        if self.date.date() > datetime.today().date() - timedelta(days=30):
            self.keep_existing_file = False
            self.server_url = ['ftp://ftp.ncep.noaa.gov']
            self.server_path = ['pub/data/nccf/com/nam/prod/']
        else:
            self.keep_existing_file = False
            self.server_url = ['ftp://nomads.ncdc.noaa.gov']
            self.server_path = ['NAM/Grid218/'+str(self.date.year)+\
                                str(self.date.month).zfill(2)+'/'+str(self.date.year)+\
                                str(self.date.month).zfill(2)+str(self.date.day).zfill(2)]

        self.ungrib_prefix = 'NAM'


    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        filename = ''
        if self.date.date() > datetime.today().date() - timedelta(days=30):
            filename = 'nam.t'+str(self.date.hour).zfill(2)+'z.awphys'+\
                        str(self.hour).zfill(2)+'.grb2.tm00'
        else:
            filename = 'nam_218_'+str(self.date.year)+str(self.date.month).zfill(2)+\
                        str(self.date.day).zfill(2)+'_'+str(self.date.hour).zfill(2)+\
                        '00_0' + str(self.hour).zfill(2) + '.grb'

        return filename
