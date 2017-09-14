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

class InputDataSetSSTNCEP(InputDataSet):
    '''
    NCEP Sea Surface Temperature (SST) input data set file in grib format.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.type = 'SST-NCEP'
        self.path = path+'/SST-NCEP'
        self.intervalseconds = 86400
        self.is_rda = [False]
        self.is_sst = True
        self.date_sst = self.date
        if 'is_analysis' in args:
            is_analysis = args['is_analysis']
            self.is_analysis = is_analysis
        if self.is_analysis:
            self.date = self.date + timedelta(hours=self.hour)
        self.name = self.get_filename()
        self.name_prepared = self.get_filename()
        if self.date.date() == datetime.today().date():
            self.keep_existing_file = False
            self.server_url = ['ftp://ftp.ncep.noaa.gov']
            self.date_sst = self.get_sst_date()
            self.server_path = ['pub/data/nccf/com/gfs/prod/sst.'+str(self.date_sst.year)+\
                                str(self.date_sst.month).zfill(2)+str(self.date_sst.day).zfill(2)]
        else:
            self.keep_existing_file = True
            self.server_url = ['ftp://polar.ncep.noaa.gov']
            self.server_path = ['pub/history/sst/ophi']

        self.ungrib_prefix = 'SSTNCEP'


    def get_sst_date(self):
        '''
        Return the date to use for acquisition of the sst
        '''
        return self.date_sst


    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        filename = ''
        if self.date.date() == datetime.today().date():
            filename = 'rtgssthr_grb_0.083.grib2'
        else:
            self.date_sst = self.date
            filename = 'rtg_sst_grb_hr_0.083.'+str(self.date_sst.year)+\
                        str(self.date_sst.month).zfill(2) + str(self.date_sst.day).zfill(2)

        return filename



class InputDataSetSSTOISST(InputDataSet):
    '''
    NOAA OI SST V2 Sea Surface Temperature (SST) input data set file in grib format.
    This is a daily sst.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.type = 'SST-OISST'
        self.intervalseconds = 86400
        self.path = path+'/SST-NOAAOI'
        self.is_sst = True
        self.is_rda = [False]
        if 'is_analysis' in args:
            is_analysis = args['is_analysis']
            self.is_analysis = is_analysis
        if self.is_analysis:
            self.date = self.date + timedelta(hours=self.hour)
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.keep_existing_file = True
        self.server_url = ['http://www.ncei.noaa.gov', 'ftp://eclipse.ncdc.noaa.gov']
        self.server_path = ['data/sea-surface-temperature-optimum-interpolation/'
                            'access/avhrr-only/'+str(self.date.year)+\
                             str(self.date.month).zfill(2), '/pub/oisst/NetCDF/'+\
                             str(self.date.year)+'/AVHRR/']
        self.ungrib_prefix = 'SSTOI'
        self.ungrib = True

    def get_sst_date(self):
        '''
        Return the date to use for acquisition of the sst
        '''
        return self.date

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        #get the delta from     current time:
        deltadate = datetime.today().date() - self.date.date()
        name_out = ''
        #There seams to be 17 days of _preliminary files... Not sure why 17 but there you go.
        if deltadate.days < 17:
            name_out = 'avhrr-only-v2.'+str(self.date.year)+str(self.date.month).zfill(2)+\
                       str(self.date.day).zfill(2)+'_preliminary.nc'
        else:
            name_out = 'avhrr-only-v2.'+str(self.date.year)+str(self.date.month).zfill(2)+\
                       str(self.date.day).zfill(2)+'.nc'

        return name_out

    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''
        return self.get_filename()+'.grb2'

    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''
        try:
            os.chdir(self.path)
            for filename in glob.glob('*.nc'):
                logging.info('WPS: Converting '+filename+' netCDF to GRIB2 file for WPS')
                process = subprocess.Popen(['ncks', '-3', '-v', 'sst', filename,
                                            'temp.nc'])
                process.wait()

                process = subprocess.Popen(['cdo', '-f', 'grb2', 'copy',
                                            'temp.nc', 'temp.grb2'])
                process.wait()

                process = subprocess.Popen(['wgrib2', 'temp.grb2', '-set_var',
                                            'TMP', '-grib', 'temp2.grb2'])
                process.wait()

                process = subprocess.Popen(['wgrib2', 'temp2.grb2', '-set_lev',
                                            'surface', '-grib', filename+'.grb2'])
                process.wait()

                os.remove('temp.nc')
                os.remove('temp.grb2')
                os.remove('temp2.grb2')
        except:
            logging.warning('OISST prepare failure')


class InputDataSetSSTJPL(InputDataSet):
    '''
    A class defining a Sea Surface Temperature (SST) input data set file in grib format.
    This goes back to 2010
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.type = 'SST-JPL'
        self.path = path+'/SST-JPL'
        self.is_sst = True
        if 'is_analysis' in args:
            is_analysis = args['is_analysis']
            self.is_analysis = is_analysis
        if self.is_analysis:
            self.date = self.date + timedelta(hours=self.hour)
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.server_url = ['ftp://podaac-ftp.jpl.nasa.gov']
        self.server_path = ['allData/ghrsst/data/L4/GLOB/JPL_OUROCEAN/G1SST/'+\
                            str(self.date.year)+'/'+str(self.date.strftime("%j")).zfill(3)]
        self.ungrib_prefix = 'SSTJPL'
        self.is_rda = [False]

    def get_sst_date(self):
        '''
        Return the date to use for acquisition of the sst
        '''
        return self.date

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        return str(self.date.year)+str(self.date.month).zfill(2)+str(self.date.day).zfill(2)+\
               '-JPL_OUROCEAN-L4UHfnd-GLOB-v01-fv01_0-G1SST.nc.bz2'


    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''

        return 'JPL_SST_'+str(self.date.year)+str(self.date.month).zfill(2)+\
                str(self.date.day).zfill(2)+'.grb2'


    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''
        pre_processing_input_dir = ''
        lon_min = []
        lon_max = []
        lat_min = []
        lat_max = []

        if args:
            pre_processing_input_dir = args['pre_processing_input_dir']
            lon_min = args['lon_min']
            lon_max = args['lon_max']
            lat_min = args['lat_min']
            lat_max = args['lat_max']

        else:
            logging.error('Error prepare will not function correctly for '+ str(self.type))

        os.chdir(self.path)
        try:
            for filename in glob.glob('*.bz2'):

                process = subprocess.Popen(['bunzip2', filename])
                process.wait()

                logging.info('WPS: Extracting latitude/longitude'
                             ' bounding box from JPL SST with max lat ')

                process = subprocess.Popen(['ncea', '-d', 'lat,'+\
                                             str(lat_min[0])+','+str(lat_max[0]),
                                            '-d', 'lon,'+str(lon_min[0])+','+str(lon_max[0]),
                                            filename[0:-4], 'sst.nc'])
                process.wait()

                logging.info('WPS: Running Poisson Interpolation to fill land mask')
                process = subprocess.Popen(['ncl', pre_processing_input_dir+\
                                            '/interpolate_SST-JPL.ncl'])
                process.wait()

                process = subprocess.Popen(['ncrename', '-O', '-v', 'SST,T', 'interp.nc'])
                process.wait()
                process = subprocess.Popen(['ncks', '-3', '-v', 'lat,lon,T,time',
                                            'interp.nc', 'sst1.nc'])
                process.wait()

                logging.info('WPS: Converting netCDF to GRIB2 file for WPS')
                process = subprocess.Popen(['cdo', '-a', '-f', 'grb2', 'copy',
                                            'sst1.nc', 'sst1.grb2'])
                process.wait()

                logging.info('WPS: Inverting latitudes in GRIB2 file')
                process = subprocess.Popen(['cdo', 'invertlat', 'sst1.grb2', 'sst2.grb2'])
                process.wait()

                process = subprocess.Popen(['wgrib2', 'sst2.grb2', '-set_center',
                                            '7', '-grib_out', filename[0:-4]+'.grb2'])
                process.wait()

                os.remove('interp.nc')
                os.remove('sst.nc')
                os.remove('sst1.nc')
                os.remove('sst1.grb2')
                os.remove('sst2.grb2')

        except:
            logging.warning('JPL prepare failure')



class InputDataSetSSTSPORT(InputDataSet):
    '''
    A class defining a Sea Surface Temperature (SST) input data set file in grib2 format.
    available at 06 and 18 utc.
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, date, hour, path, **args):
        InputDataSet.__init__(self, date, hour, path, **args)
        self.type = 'SST-SPORT'
        self.path = path+'/SST-SPORT'
        self.is_sst = True
        if 'is_analysis' in args:
            is_analysis = args['is_analysis']
            self.is_analysis = is_analysis
        if self.is_analysis:
            self.date = self.date + timedelta(hours=self.hour)
        self.is_rda = [False, False]
        self.name = self.get_filename()
        self.name_prepared = self.get_filename_prepared()
        self.server_url = ['ftp://geo.msfc.nasa.gov', 'http://soostrc.comet.ucar.edu']
        self.server_path = ['SPoRT/sst/northHemis/grib2', 'data/grib/sst']
        self.ungrib_prefix = 'SSTSPORT'
        self.server_pos = 0

    def get_filename(self):
        '''
        Generate a filename to download for this dataset for the time given.
        '''
        sst_date = self.get_sst_date()

        if self.server_pos == 0:
            sst_date_string = str(sst_date.year)+str(sst_date.month).zfill(2)+\
                              str(sst_date.day).zfill(2)+'_'+str(sst_date.hour).zfill(2)+\
                              '00_sport_nhemis_sstcomp.grb2.gz'
        else:
            sst_date_string = str(sst_date.year)+str(sst_date.month).zfill(2)+\
                              str(sst_date.day).zfill(2)+'_'+str(sst_date.hour).zfill(2)+\
                              '00_sport_nhemis_sstcomp.grb2'

        return sst_date_string

    def get_filename_prepared(self):
        '''
        Generate a filename for the processed output file for a given time
        for this dataset
        '''
        sst_date = self.get_sst_date()
        sst_date_string = 'SPORT_SST_'+str(sst_date.year)+str(sst_date.month).zfill(2)+\
                           str(sst_date.day).zfill(2)+'.'+str(sst_date.hour).zfill(2)+'Z.grb2'
        return sst_date_string


    def get_sst_date(self):
        '''
        Work out the date / time for the sst.
        '''
        ssthour = (self.date.hour - ((self.date.hour +6) % 12)+12) % 24
        sst_date = self.date - timedelta(days=1) - timedelta(hours=self.date.hour)+\
                   timedelta(hours=ssthour)

        logging.info('SPORT SST hour calculated as '+str(sst_date))
        return sst_date


    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''

        pre_processing_input_dir = ''
        #If prepare has been called properly
        if args:
            pre_processing_input_dir = args['pre_processing_input_dir']
        else:
            logging.error('Error prepare will not function correctly for '+ str(self.type))


        glob_txt = '*.gz'
        if self.server_pos == 1:
            glob_txt = '*.grb2'

        try:
            os.chdir(self.path)
            for filename in glob.glob(glob_txt):

                if self.server_pos == 0:
                    process = subprocess.Popen(['gunzip', filename])
                    process.wait()

                logging.info('WPS: SST-SPORT: Converting GRIB2 file to netCDF file for processing')
                process = subprocess.Popen(['wgrib2', filename[0:-3], '-netcdf', 'sst.nc'])
                process.wait()

                logging.info('WPS: SST-SPORT: Assigning attributes to TMP_surface')
                process = subprocess.Popen(['ncatted', '-O', '-a',
                                            '_FillValue,TMP_surface,o,f,-9999', 'sst.nc'])
                process.wait()

                logging.info('WPS: Running Poisson Interpolation to fill land mask')
                process = subprocess.Popen(['ncl', pre_processing_input_dir+\
                                            '/interpolate_SST-SPORT.ncl'])
                process.wait()

                process = subprocess.Popen(['ncrename', '-O', '-v', 'SST,T', 'interp.nc'])
                process.wait()

                process = subprocess.Popen(['ncks', '-3', '-v', 'latitude,longitude,T,time',
                                            'interp.nc', 'sst1.nc'])
                process.wait()

                logging.info('WPS: Converting netCDF back to GRIB2 for WPS')
                process = subprocess.Popen(['cdo', '-a', '-f', 'grb2', 'copy',
                                            'sst1.nc', 'sst1.grb2'])
                process.wait()

                logging.info('WPS: Inverting latitudes in GRIB2 file')
                process = subprocess.Popen(['cdo', 'invertlat', 'sst1.grb2',
                                            'sst2.grb2'])
                process.wait()

                process = subprocess.Popen(['wgrib2', 'sst2.grb2', '-set_center',
                                            '7', '-grib_out', filename[0:-3]+'.grb2'])
                process.wait()

                os.remove('interp.nc')
                os.remove('sst.nc')
                os.remove('sst1.nc')
                os.remove('sst1.grb2')
                os.remove('sst2.grb2')
                os.remove(filename[0:-3])

        except:
            logging.warning('SPORT prepare failure')
