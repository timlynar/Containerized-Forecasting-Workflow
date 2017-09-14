#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
IBM Containerized Forecasting Workflow

DESCRIPTION

    WPS and WRF workflow for weather simulation.

AUTHOR

    Timothy Lynar <timlynar@au1.ibm.com>, IBM Research, Melbourne, Australia
    Frank Suits <frankst@au1.ibm.com>, IBM Research, Melbourne, Australia; Dublin, Ireland; Yorktown, USA
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

VERSION
    o1.8
"""

from datetime import datetime, timedelta
from math import pi, cos
import os
import logging
from multiprocessing import Process, Queue, cpu_count
import shutil
import subprocess
import pytz
from netCDF4 import Dataset
from inputdataset import InputDataSet
from datasets_aux import *
from datasets_fcst import *
from datasets_hist import *
from datasets_sst import *
import util

class Stevedore(object):
    '''
    IBM Containerized Forecasting Workflow - WPS and WRF workflow for weather simulations.
    '''

    #Log level for items printed to the screen.
    SCREEN_LOG_LEVEL = logging.INFO

    #Number of workers to use when downloading data. When using RDA I suggest you set this to 1.
    DOWNLOAD_WORKERS = 1

    #Max number of domains supported by namelist instrumentation
    MAXINSTRUMENTEDDOMAINS = 4

    #Seconds in an hour
    SEC_IN_HOUR = 3600

    #The defualt dataset. If no dataset is added this will be used.
    DEFAULT_DS = "GFS"

    #The default history interval aka the time between output files in minutes.
    DEFAULT_HIST_INT = 60


    def __init__(self, datetimeStart, forecastLength, latitude, longitude, ncores=4, ndomains=3, timestep=10,
                 gridratio=3, gridspacinginner=1.5, ngridew=100, ngridns=100, nvertlevels=40, phys_mp=17, phys_ralw=4,
                 phys_rasw=4, phys_cu=1, phys_pbl=1, phys_sfcc=1, phys_sfc=2, phys_urb=0, wps_map_proj='lambert', runshort=0,
                 auxhist7=False, auxhist2=False, feedback=False, adaptivets=False, projectdir='default', norunwrf=False, is_analysis=False,
                 altftpserver=None, initialConditions=['GFS'], boundaryConditions=['GFS'], inputData=[], tsfile=None, history_interval=60):
        '''
        Constructor
        '''
        #IBM-NOTICE
        self.notice = "IBM Containerized Forecasting Workflow \n Licensed Materials - Property of IBM \n  Copyright IBM Corp. 2017 ALL RIGHTS RESERVED \n "

        print self.notice
        #End IBM Notice.


        #Import the root path of this DeepThunder installation form the environment variable DEEPTHUNDER_ROOT
        self.directory_root_input = os.environ.get('DEEPTHUNDER_ROOT')

        #If the root path of the DeepThunder installation is not set then guess that it will be /opt/deepthunder
        if self.directory_root_input is None:
            self.directory_root_input = '/opt/deepthunder'

        #Specify the directory containing the InputDataSets have been stored after download
        self.directory_root_inputDataSets = self.directory_root_input+'/data/inputDataSets'

        #Specify the directory where the DeepThunder run directory will be created
        self.directory_root_run = self.directory_root_input+'/data/domains'

        #Specify the the directory containing the databases on terrestrial information
        self.directory_root_geog = self.directory_root_input+'/data/Terrestrial_Input_Data'

        #Specify the the number of cores available to execute Real.exe and WRF.exe.
        #They will run in parallel with this number
        self.numberCores = ncores

        #Define a list that will contain the domains to be processed
        self.domains = range(1, ndomains+1)

        #Specify the number of domains in this run
        self.maxdom = ndomains

        #Set run length in hours for WPS
        self.runlength_wps = None

        #Set end time / date for WPS
        self.datetimeEndUTC_wps = None

        #Grid spacing dx
        #lambert or any other projection except lat-lon.
        #gets gridspacinginner from km to meters. and sets it for the outside domain.
        #self.dx is the value for the outermost domain = domain 1.
        #outerdx = innerdx * 1000 = innnerdx in m * (gridratio  * )
        self.dx = (gridspacinginner*1000*(gridratio**(ndomains-1)))
        self.wpsdx = self.dx


        #Grid spacing dy (assume it is the same as dx.)
        self.dy = self.dx
        self.wpsdy = self.dy

        #Parent grid ratio
        self.parent_grid_ratio = gridratio

        #Number of vertical levels
        self.num_vertical_levels = nvertlevels

        #Define a list that will contain the domains not just those to be processed
        # in descending order
        self.idomains = range(self.MAXINSTRUMENTEDDOMAINS, 0, -1)

        #Physics options
        #Micro Physics vairables
        self.phys_mp_val = phys_mp
        #Radiation Long wave Physics (ra_lw)
        self.phys_ralw_val = phys_ralw
        #Radiation Short wave Physics (ra_sw)
        self.phys_rasw_val = phys_rasw
        #Cumulus scheme - populate as list
        self.phys_cu_val = util.convert_to_list(phys_cu, self.maxdom)
        #Planetary boundary layer (PBL)
        self.phys_pbl_val = phys_pbl
        #Surface Layer Options (sf_sfclay_physics)
        self.phys_sfcc_val = phys_sfcc
        #Land Surface Options (sf_surface_physics)
        self.phys_sfc_val = phys_sfc
        #Urban Surface Options (sf_urban_physics)
        self.phys_urb_val = phys_urb
        #END physics options

        #interval_seconds
        self.sstintervalseconds = 0
        self.maxintervalseconds = 0
        self.WPSintervalseconds = None

        #Set the number of grid points in each domain
        self.domain_dims_nx = util.convert_to_list(ngridew, self.maxdom)
        self.domain_dims_ny = util.convert_to_list(ngridns, self.maxdom)

        #Set the history_interval
        self.domain_history_interval = util.convert_to_list(history_interval, self.maxdom)

        #Store in a list the sizes of the elements of the numerical grid of each domain in longitude direction
        self.domain_dims_dx = []

        #Store in a list the sizes of the elements of the numerical grid of each domain in latitude direction
        self.domain_dims_dy = []

        #WPS map projection
        self.wps_map_proj = wps_map_proj

        #Dictionary to store the required input data sets
        self.inputDataSets = {}

        #Dataset for initial conditions
        self.initialConditions = initialConditions

        #Dataset for boundary conditions
        self.boundaryConditions = boundaryConditions

        #Timestep used for forecast
        self.timeStepForecast = timestep

        #Load the inputDataSets and populate dictionary key = name i.e 'GFS' value = inputDataSet
        #make sure that inputData = self.initialConditions + self.boundaryConditions
        #Adding initialConditions to inputData.
        for ds in self.initialConditions:
            inputData.append(ds)

        #Adding boundaryConditions to inputData.
        for ds in self.boundaryConditions:
            inputData.append(ds)

        #After adding both initialConditions and boundaryConditions
        #if inputData is still of zero size then add DEFAULT_DS
        if not inputData:
            print 'Added default dataset '+ str(self.DEFAULT_DS)
            inputData.append(self.DEFAULT_DS)

        #Adds all datasets to the dictionary inputDataSets
        for ds in inputData:
            print 'Add dataset '+ str(ds)
            self.inputDataSets[str(ds)] = None

        #Store all input files as inputDataSet
        self.inputfiles = []

        #Store forecastLength in hours
        self.forecastLength = forecastLength

        #Run short is the number of hours that you wish to prep for but not execute with wrf.
        #wrf run time is set to forecastLength - runshort
        self.runshort = runshort

        #Store latitude of centre coordinates
        self.latitude = util.convert_to_list(latitude, self.maxdom)

        #Store longitude of centre coordinates
        self.longitude = util.convert_to_list(longitude, self.maxdom)

        #aux hist variables (true for turn on false for turn off)
        self.auxhist7 = auxhist7
        self.auxhist2 = auxhist2

        #Feedback on or off
        self.feedback = feedback

        #Turn on or off adaptive time Steps
        self.adaptivets = adaptivets

        #Set the project directory output data will be found in /data/domains/$projectDir/
        self.project_dir = projectdir

        #Set a flag to run wrf. If not norunwrf run wrf
        self.norunwrf = norunwrf

        #Set flag for this being a reanalysis - this effects how the SST is used.
        #Use this option if you need to do a long run historical simulation.
        self.is_analysis = is_analysis

        #alt ftp. - download all data from this server if set.
        self.alt_ftp_server_url = altftpserver

        #Define a pytz time zone object for Coordinated Universal Time (UTC)
        utc = pytz.utc
        #Create a datetime object for the forecast start time in local time zone
        self.datetimeStartUTC = datetime(year=datetimeStart.year, month=datetimeStart.month,
                                         day=datetimeStart.day, hour=datetimeStart.hour, tzinfo=utc)

        #Create a datetime object for the forecast end time in local time zone
        #self.datetimeEndUTC = self.datetimeStartUTC + timedelta(days=delta[0]) + timedelta(hours=delta[1])
        self.datetimeEndUTC = self.datetimeStartUTC+timedelta(hours=self.forecastLength)

        #Get the SST date (previous day) #will be updated latter
        self.datetimeSST = self.datetimeStartUTC-timedelta(days=1)

        #Time series file
        self.tsfile = tsfile

        #Set the directory structure of the Stevedore installation assuming Stevedore has been built with the Dockerfile provided
        self.directory_WPS_input = self.directory_root_input+'/externalDependencies/src/WPS'
        self.directory_WRF_input = self.directory_root_input+'/externalDependencies/WRF'
        self.directory_IBM_input = self.directory_root_input+'/externalDependencies/src/IBM'

        self.directory_PreProcessing_input = self.directory_root_input+'/PreProcessing'

        #Set the sub-directories of the DeepThunder run directory
        self.directory_run = self.directory_root_run+'/'+str(self.project_dir)+'/'+str(self.datetimeStartUTC.year)+'-'+\
                             str(self.datetimeStartUTC.month).zfill(2)+'-'+str(self.datetimeStartUTC.day).zfill(2)+'_'+\
                             str(self.datetimeStartUTC.hour).zfill(2)

        self.directory_data = self.directory_root_input+'/data'
        self.directory_PreProcessing_run = self.directory_run+'/PreProcessing'
        self.directory_wrf_run = self.directory_run+'/WRF'

        #LOGFILE
        self.logfile = self.directory_run+'/IBM-CFW-logfile.log'

        #Create lists to store minimum and maximum latitude and longitude of all the domains, in the same order as self.domains
        #Note this bounding box is only used for data acquisition with GFSsubset.
        self.lat_min = []
        self.lat_max = []
        self.lon_min = []
        self.lon_max = []

        #Calculate bounding box for each domain and dx and dy
        for domain in self.domains:
            #calculate dx and dy per domain in km
            domspace = round(gridspacinginner*1000*gridratio**(self.maxdom-domain))
            self.domain_dims_dx.append(domspace)
            self.domain_dims_dy.append(domspace)

            self.lat_min.append(round(self.latitude[domain-1]-1.5*self.domain_dims_ny[domain-1]*self.domain_dims_dy[domain-1] / 2.0 / 1000.0 / 111.325, 2))
            self.lat_max.append(round(self.latitude[domain-1]+1.5*self.domain_dims_ny[domain-1]*self.domain_dims_dy[domain-1] / 2.0 / 1000.0 / 111.325, 2))
            self.lon_min.append(round(self.longitude[domain-1]-1.5*self.domain_dims_nx[domain-1]*self.domain_dims_dx[domain-1] / 2.0 / 1000.0 / (cos(self.latitude[domain-1]/360.0*(2.0*pi)) * 111.325), 2))
            self.lon_max.append(round(self.longitude[domain-1]+1.5*self.domain_dims_nx[domain-1]*self.domain_dims_dx[domain-1] / 2.0 / 1000.0 / (cos(self.latitude[domain-1]/360.0*(2.0*pi)) * 111.325), 2))


        #Create the top-level run directory if it does not exist
        if not os.path.exists(self.directory_run):
            os.makedirs(self.directory_run)

        #Initialise the OpenDeepThunder log-file
        if os.path.exists(self.logfile):
            os.remove(self.logfile)

        #Create the logging instance for this DeepThunder object
        logging.basicConfig(filename=self.logfile, level=logging.DEBUG,
                            format='%(asctime)s: %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')

        console = logging.StreamHandler()
        console.setLevel(self.SCREEN_LOG_LEVEL)
        formatter = logging.Formatter('%(asctime)s: %(message)s')
        console.setFormatter(formatter)
        logging.getLogger().addHandler(console)

        logging.info('Construct IBM Containerized Forecasting Workflow object.')

        #If the environment variable has not been set fire off a warning.
        if self.directory_root_input is None:
            logging.warning("The environment variable DEEPTHUNDER_ROOT has not been set so /opt/deepthunder will be used in its place")

        logging.info("Domain. "+ str(self.domains)+ " "+str(self.latitude)+" "+str(self.longitude)+ " " + str(self.domain_dims_ny)  + " " + str(self.domain_dims_dx))



    def check_input_data(self):
        """
        Determines the file names of required input data and downloads it.
        """

        #Log entering check_input_data
        logging.info('check_input_data: Download input data.')

        #Create the directory to store the data we download if it does not already exist.
        if not os.path.exists(self.directory_root_inputDataSets):
            os.mkdir(self.directory_root_inputDataSets)

        #Set up multiprocessing
        workers = self.DOWNLOAD_WORKERS
        work_queue = Queue()
        done_queue = Queue()
        processes = []
        baddata = []

        for ids in self.inputDataSets:
            try:
                idsMethodName = eval('InputDataSet'+str(ids))
                logging.debug('check_input_data: Download input data. with '+ str(idsMethodName))
                inputDataSet = idsMethodName(self.datetimeStartUTC, 0, self.directory_root_inputDataSets, is_analysis=self.is_analysis)
                intervalhours = inputDataSet.intervalseconds/self.SEC_IN_HOUR

                #Set the intervalseconds to the lowest of the datasets.
                if inputDataSet.intervalseconds > self.maxintervalseconds:
                    self.maxintervalseconds = inputDataSet.intervalseconds

                #If the dataset is an SST and we are not running an analysis then update the SST date
                if inputDataSet.is_sst and not self.is_analysis:
                    try:
                        self.datetimeSST = inputDataSet.getSSTDate()
                        logging.debug('SST date set to '+str(self.datetimeSST))
                    except:
                        logging.error('Error malformed InputDataSet. SSTDate will be set to default -1 day')


                #Store input dataset object also as attribute of the DeepThunder object
                self.inputDataSets[ids] = (inputDataSet)

                #For hours in the simulation length + 1 / the interval between files in hours.
                for hour_steps in range((self.forecastLength/intervalhours)+1):
                    #Create a input dataset object
                    #note we pass the hour steps rather than the new date as this depends on the dataset if we increment the date etc.
                    inputDataSet = idsMethodName(self.datetimeStartUTC, hour_steps*intervalhours,
                                                 self.directory_root_inputDataSets, lon_min=self.lon_min[0],
                                                 lon_max=self.lon_max[0], lat_min=self.lat_min[0], lat_max=self.lat_max[0], is_analysis=self.is_analysis)

                    #Store input dataset object also as attribute of the DeepThunder object
                    work_queue.put(inputDataSet)
                    #Store input dataset object also as attribute of the DeepThunder object
                    self.inputfiles.append(inputDataSet)
                    #Log information to DeepThunder log-file
                    logging.debug(str(ids)+ ' filename: '+ inputDataSet.get_filename())
            except:
                logging.error('ERROR: ' + str(ids)+ ' may not be a recognised dataset. It will not be used.')
                baddata.append(ids)

        #Delete all the bad datasets to prevent future errors.
        for ids in baddata:
            logging.error('Removing : ' + str(ids)+ ' from datasets')
            del self.inputDataSets[ids]

        #Work on work_queue
        for worker_id in range(workers):
            try:
                #
                logging.debug('check_input_data: starting worker ' + str(worker_id))
                process = Process(target=self._worker_download_input_data, args=(work_queue, done_queue))
                process.start()
                processes.append(process)
                work_queue.put('STOP')
            except:
                logging.error("ERROR problem processing the queue")

        #Wait for it all to finish downloading.
        for process in processes:
            process.join()

        #log that we are all done.
        logging.info('check_input_data: data downloaded.')


    def _worker_download_input_data(self, work_queue, done_queue):
        """
        Downloads the file(s) defined by and inputDataSet object
        stored in the work_queue.
        """
        logging.debug('_worker_download_input_data')
        #Get next object waiting in the work_queue

        for inputDataSet in iter(work_queue.get, 'STOP'):
            #if alt_ftp_server_url flag is set the pass it on.
            if self.alt_ftp_server_url != None:
                logging.info('_worker_download_input_data Setting alternate ftp server as '+str(self.alt_ftp_server_url)+ 'for '+ str(inputDataSet.name))
                inputDataSet.alt_server_url = str(self.alt_ftp_server_url)

            #Download the input data set file
            inputDataSet.download()



    def run_preprocessing(self):
        """
        Pre-processes the input data for a WRF simulation. It calls functions
        that execute the WRF Pre-processing System and real.exe.
        """

        #Log information to DeepThunder log-file
        logging.info('Run pre-processing ...')

        #Delete the run directory of the pre-processing if it exists
        if os.path.exists(self.directory_PreProcessing_run):
            shutil.rmtree(self.directory_PreProcessing_run)

        #Round up the end time for WPS based on grib frequency (even though forecast length is shorter than the frequency)
        #Example: 1 hour run = forecast length, but grib file every 3 hourly or 6 hourly
        #Get the number of full days and remaining hours.
        delta = divmod(self.forecastLength, 24)
        thours = timedelta(hours=delta[1])
        #What is larger the remainder of hours or self.intervalseconds Note: self.maxintervalseconds is the maximum of intervalseconds of all datasets used.
        maxinterval = max(thours.seconds, self.maxintervalseconds)
        #Set these interval hours as maxintervalhours
        maxintervalhours = maxinterval/self.SEC_IN_HOUR
        #NOTE: TL. Need to make this more elegant
        #Set the end-time by using maxintervalhours
        self.datetimeEndUTC_wps = self.datetimeStartUTC+timedelta(days=delta[0])+timedelta(hours=maxintervalhours)
        if self.datetimeEndUTC_wps > self.datetimeEndUTC:
            self.datetimeEndUTC_wps = self.datetimeEndUTC

        rwps = self.datetimeEndUTC_wps - self.datetimeStartUTC  # Input file frequency based run length for ungrib, metgrid and real
        rwps_fdays = rwps.days * 24 # hours
        rwps_fsec = rwps.seconds / self.SEC_IN_HOUR # hours
        self.runlength_wps = rwps_fdays+rwps_fsec

        #If input data set for initial and boundary conditions are the same
        if self.initialConditions == self.boundaryConditions:

            #Run the WRF Pre-Processing System for the initial and boundary conditions
            self._run_WPS(self.directory_PreProcessing_run+'/WPS_boundary',
                          self.inputDataSets, self.datetimeEndUTC_wps)

            #Run the real.exe for the initial and boundary conditions
            self._run_Real(self.directory_PreProcessing_run+'/Real_boundary',
                           self.directory_PreProcessing_run+'/WPS_boundary',
                           self.boundaryConditions, self.datetimeEndUTC_wps)

            #If input data set for initial and boundary conditions are different
        else:
            #Create a dummy copy of the input data sets
            dsUngrib = self.inputDataSets.copy()

            #Remove the input data set for the initial conditions
            for ids in self.initialConditions:
                dsUngrib.pop(ids, None)

            #Run the WRF Pre-processing System for the boundary conditions
            self._run_WPS(self.directory_PreProcessing_run+'/WPS_boundary',
                          dsUngrib, self.datetimeEndUTC_wps)

            #Run the real.exe for the boundary conditions
            self._run_Real(self.directory_PreProcessing_run+'/Real_boundary',
                           self.directory_PreProcessing_run+'/WPS_boundary',
                           self.boundaryConditions, self.datetimeEndUTC_wps)

            #Create a dummy copy of the input data sets
            dsUngrib = self.inputDataSets.copy()

            #Remove the input data set for the boundary conditions
            for ids in self.boundaryConditions:
                dsUngrib.pop(ids, None)

            #Run the WRF Pre-processing System for the initial conditions
            self._run_WPS(self.directory_PreProcessing_run+'/WPS_initial',
                          dsUngrib, self.datetimeStartUTC)

            #Run the real.exe for the initial conditions
            self._run_Real(self.directory_PreProcessing_run+'/Real_initial',
                           self.directory_PreProcessing_run+'/WPS_initial',
                           self.initialConditions, self.datetimeStartUTC)




    def _replace_location_strings(self, fname):
        """
        Replaces all the strings in the namelist to do with the location of the domain.
        """

        #Go through the datasets and find the one with the smallest interval
        self.WPSintervalseconds = None
        for ids in self.inputDataSets.iterkeys():
            idso = self.inputDataSets[ids]
            if idso.intervalseconds < self.WPSintervalseconds or self.WPSintervalseconds is None:
                self.WPSintervalseconds = idso.intervalseconds

        #Replace the place-holders in the namelist file with the properties of this DeepThunder object
        util.replace_string_in_file(fname, 'DT_LATITUDE_DT', str(self.latitude[0]))
        util.replace_string_in_file(fname, 'DT_LONGITUDE_DT', str(self.longitude[0]))
        util.replace_string_in_file(fname, 'DT_GEOG_DATA_PATH_DT', str(self.directory_root_geog))
        util.replace_string_in_file(fname, 'DT_MAX_DOM_DT', str(max(self.domains)))

        dx = self.wpsdx
        dy = self.wpsdy

        if dx == 0:
            dx = self.domain_dims_dx[0]

        if dy == 0:
            dy = self.domain_dims_dy[0]


        #If we use lat-lon then dx and dy will be in degrees.
        if self.wps_map_proj == 'lat-lon':
            #Round to two decimal places.
            #note dx and dy = distance in m.

            #dxm and dy in km
            dxkm = self.dx / 1000
            dykm = self.dy / 1000

            # km per deg at the equator.
            kmperdeg = 111.1774799

            dx_deg = (dxkm  / kmperdeg)
            dy_deg = (dykm / kmperdeg)

            #set the dx
            dx = dx_deg
            dy = dy_deg


        util.replace_string_in_file(fname, 'DT_DX_1_DT,', str(dx))
        util.replace_string_in_file(fname, 'DT_DY_1_DT,', str(dy))
        util.replace_string_in_file(fname, 'DT_PARENT_GRID_RATIO_DT', str(self.parent_grid_ratio))
        util.replace_string_in_file(fname, 'DT_INTERVAL_SECONDS', str(self.WPSintervalseconds))
        util.replace_string_in_file(fname, 'DT_WPS_MAP_PROJ_DT', str(self.wps_map_proj))

        #TODO: TL This is not working. NOTE lon is not used.
        # special handling for staggered domains with different centre lat/lon values
        centered = (min(self.latitude) == max(self.latitude)) & (min(self.longitude) == max(self.longitude))
        starti = [1]*len(self.domains)
        startj = starti


        for i in range(1, len(self.domains)):
            starti[i] = int(self.domain_dims_nx[i-1]*(1-1.0/self.parent_grid_ratio)/2+1)
            startj[i] = int(self.domain_dims_ny[i-1]*(1-1.0/self.parent_grid_ratio)/2+1)

            #Apply offset of domain relative to parent, based on relative lat/lon
            if not centered:
                lat = self.latitude[0]
                lon = self.longitude[0]
                coslat = cos(lat*pi/180)
                kmperdeg = 111.2  # this is not meant to be exact
                for i in range(1, len(self.domains)):
                    dx = self.domain_dims_dx[i]
                    dy = self.domain_dims_dy[i]

                    #This calculates the difference in km between the prior domain centre and the current domain centre
                    shiftx = ((self.longitude[i]-self.longitude[i-1])*coslat*kmperdeg)
                    shifty = ((self.latitude[i]-self.latitude[i-1])*kmperdeg)

                    #This should point to the lower left hand corner of this domain in its parent domain coordinates.
                    #this gives us the middle. Now we need to subtract its length and width in parent points.
                    #starti[i] = starti[i-1] + (round(shiftx/(dx/1000))) * -1
                    #startj[i] = startj[i-1] + (round(shifty/(dy/1000))) * -1
                    pointsx = (round(shiftx/(dx/1000)))
                    pointsy = (round(shifty/(dy/1000)))

                    #Calculate the bottom left based on the shift.
                    #points to move x = (parent width - current width) /2 + pointsx
                    #points to move y = (parent width - current width) /2 + pointsx

                    starti[i] = starti[i-1]+((self.domain_dims_nx[i-1]-(self.domain_dims_nx[i]/dx))/2)+pointsx
                    startj[i] = startj[i-1]+((self.domain_dims_ny[i-1]-(self.domain_dims_ny[i]/dx))/2)+pointsy

        #Set values for the actual domain range in use
        for i in self.domains:
            util.replace_string_in_file(fname, 'DT_WE_COUNT_%d_DT'%i, str(self.domain_dims_nx[i-1]))
            util.replace_string_in_file(fname, 'DT_SN_COUNT_%d_DT'%i, str(self.domain_dims_ny[i-1]))

            if i > 1:
                nstarti = int(starti[i-1])
                nstartj = int(startj[i-1])
                util.replace_string_in_file(fname, 'DT_I_PARENT_START_%d_DT'%i, str(nstarti))
                util.replace_string_in_file(fname, 'DT_J_PARENT_START_%d_DT'%i, str(nstartj))

        #Fill the remaining non-used domains with numbers - to replace the text template values
        for i in self.idomains:
            if i > max(self.domains):
                util.replace_string_in_file(fname, 'DT_WE_COUNT_%d_DT'%i, str(0))
                util.replace_string_in_file(fname, 'DT_SN_COUNT_%d_DT'%i, str(0))
                util.replace_string_in_file(fname, 'DT_I_PARENT_START_%d_DT'%i, str(0))
                util.replace_string_in_file(fname, 'DT_J_PARENT_START_%d_DT'%i, str(0))


    def _run_WPS(self, directory_WPS_run, dsUngrib, datetimeEndUTC):
        """
        Prepares and runs the WRF Pre-processing System (WPS).
        """

        #Log information to DeepThunder log-file
        logging.info('_run_WPS Run WPS. Entered')

        #Check if the terrestrial input data is at self.directory_root_geog
        if os.path.exists(self.directory_root_geog):
            logging.info('_run_WPS geog data directory exists')
            #If the data does not exist then download it and extract it to where it belongs.
        else:
            logging.warning('_run_WPS the user has not setup static terrestrial input data.')
            self._download_geog_data(self.directory_root_geog, self.directory_root_input)

        #Create the run directory for WPS and all of its sub-directories
        os.makedirs(directory_WPS_run+'/geogrid/src')
        os.makedirs(directory_WPS_run+'/metgrid/src')
        os.makedirs(directory_WPS_run+'/ungrib/src')
        os.makedirs(directory_WPS_run+'/ungrib/Variable_Tables')

        #Create links to the geogrid executable and table
        util.link_to(self.directory_WPS_input+'/geogrid/src/geogrid.exe', directory_WPS_run+'/geogrid/src/geogrid.exe')
        util.link_to(self.directory_WPS_input+'/geogrid/GEOGRID.TBL.ARW', directory_WPS_run+'/geogrid/GEOGRID.TBL')

        #Create links to the metgrid executable and table
        util.link_to(self.directory_WPS_input+'/metgrid/src/metgrid.exe', directory_WPS_run+'/metgrid/src/metgrid.exe')
        util.link_to(self.directory_WPS_input+'/metgrid/METGRID.TBL.ARW', directory_WPS_run+'/metgrid/METGRID.TBL')

        #Create links to the ungrib executable and the variable tables
        util.link_to(self.directory_WPS_input+'/ungrib/src/ungrib.exe',
                     directory_WPS_run+'/ungrib/src/ungrib.exe')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.SST',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.SSTNCEP')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.SST',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.SSTOI')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.SST',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.SSTJPL')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.SST',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.SSTSPORT')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.SST',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.SSTMUR')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.ECMWF_sigma',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.ECMWF_sigma')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.RAP.hybrid.ncep',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.RAP')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.RAP_noLSM',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.RAP_noLSM')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.NAM',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.NAM')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.LIS',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.NASALISCONUS')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.ERA-interim.ml',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.ERAISFC')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.GFSNEW',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.GFSNEW')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.GFSNEW',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.GFSsubset')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.GFSRDA',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.GFSRDA')

        util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.CFSR2_web',
                     directory_WPS_run+'/ungrib/Variable_Tables/Vtable.CFSR')


        #ERAI - Vtable (model levels used). Note this Will not work if grib files contain pressure level data
        if self.inputDataSets.get('ERAI') is not None:
            util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.ERA-interim.ml',
                         directory_WPS_run+'/ungrib/Variable_Tables/Vtable.ERAI')

        #If the forecast starts on a date after January 15, 2015  use the upgraded version of the Global Forecast System (GFS)
        if self.datetimeStartUTC <= datetime(2015, 1, 15, tzinfo=pytz.utc):
            util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.GFS',
                         directory_WPS_run+'/ungrib/Variable_Tables/Vtable.GFS')
            util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.GFS',
                         directory_WPS_run+'/ungrib/Variable_Tables/Vtable.FNL')
        else:
            util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.GFSNEW',
                         directory_WPS_run+'/ungrib/Variable_Tables/Vtable.GFS')
            util.link_to(self.directory_WPS_input+'/ungrib/Variable_Tables/Vtable.GFSNEW',
                         directory_WPS_run+'/ungrib/Variable_Tables/Vtable.FNL')

        #Run the linking script of WPS
        util.link_to(self.directory_WPS_input+'/link_grib.csh', directory_WPS_run+'/link_grib.csh')

        #Copy the template for the WPS namelist from the WPS input directory to the WPS run directory
        shutil.copy(self.directory_IBM_input+'/namelist.wps', directory_WPS_run+'/namelist.wps')

        #Change to the WPS run directory
        os.chdir(directory_WPS_run)

        #Link the executables to the current directory
        util.link_to('ungrib/src/ungrib.exe', directory_WPS_run+'/ungrib.exe')
        util.link_to('geogrid/src/geogrid.exe', directory_WPS_run+'/geogrid.exe')
        util.link_to('metgrid/src/metgrid.exe', directory_WPS_run+'/metgrid.exe')

        #Replace the place-holders in the WPS namelist file with the properties of this DeepThunder object
        util.replace_string_in_file('namelist.wps', 'DT_START_DATE_TIME_DT', str(self.datetimeStartUTC.year)+'-'+str(self.datetimeStartUTC.month).zfill(2)+'-'+str(self.datetimeStartUTC.day).zfill(2)+'_'+str(self.datetimeStartUTC.hour).zfill(2)+':00:00')
        util.replace_string_in_file('namelist.wps', 'DT_END_DATE_TIME_DT', str(datetimeEndUTC.year)+'-'+str(datetimeEndUTC.month).zfill(2)+'-'+str(datetimeEndUTC.day).zfill(2)+'_'+str(datetimeEndUTC.hour).zfill(2)+':00:00')

        #Set all the location variables in namelist.wps
        self._replace_location_strings('namelist.wps')

        #For each input dataset label
        dictUngrib = []

        #For each input dataset file of this label
        for ids in self.inputDataSets.iterkeys():
            idso = self.inputDataSets[ids]
            #Call the prepare function of objects of class InputDataSet
            if idso.ungrib:
                idso.prepare(pre_processing_input_dir=self.directory_PreProcessing_input, lon_min=self.lon_min, lon_max=self.lon_max, lat_min=self.lat_min, lat_max=self.lat_max)
                if idso.ungrib:
                        #Run the ungrib function
                    if idso.name not in dictUngrib:
                        logging.info('_run_WPS Ungrib '+ str(idso.name))
                        self._ungrib(idso.type, directory_WPS_run, idso.ungrib_prefix, datetimeEndUTC)
                        dictUngrib.append(idso.name)
                    else:
                        logging.info('ids '+str(idso.name)+'requests ungrib skipped')

            else:
                logging.info('ids '+str(idso.name)+'is for verification only prepare() will not be run for this dataset')


        #If ERAI compute pressure on Model levels for real.exe
        if self.inputDataSets.get('ERAI') is not None:
            #Setup
            util.link_to(self.directory_WPS_input+'_IBM/util/ecmwf_coeffs', directory_WPS_run+'/ecmwf_coeffs')     #ecmwf_coeffs
            util.link_to(self.directory_WPS_input+'/util/src/calc_ecmwf_p.exe', directory_WPS_run+'/calc_ecmwf_p.exe') #calc_ecmwf_p.exe
            fg_name = []                                                                                                                          #'ERAI','PRES'
            fg_name.append('ERAI')
            fg_name.append('PRES')
            util.replace_string_in_file('namelist.wps', ' fg_name = \'FILE\',', ' fg_name = \''+'\', \''.join(fg_name)+'\',')
            #Run calc_ecmwf_p.exe
            process = subprocess.Popen([directory_WPS_run+'/calc_ecmwf_p.exe'])
            process.wait()

        #Change to the WPS run directory
        os.chdir(directory_WPS_run)

        #Copy again the the template of the namelist file to have an clean version
        shutil.copy(self.directory_IBM_input+'/namelist.wps', directory_WPS_run+'/namelist.wps')

        #Replace the place-holders in the WPS namelist file with the properties of this DeepThunder object
        util.replace_string_in_file('namelist.wps', 'DT_START_DATE_TIME_DT', str(self.datetimeStartUTC.year)+'-'+str(self.datetimeStartUTC.month).zfill(2)+'-'+str(self.datetimeStartUTC.day).zfill(2)+'_'+str(self.datetimeStartUTC.hour).zfill(2)+':00:00')
        util.replace_string_in_file('namelist.wps', 'DT_END_DATE_TIME_DT', str(datetimeEndUTC.year)+'-'+str(datetimeEndUTC.month).zfill(2)+'-'+str(datetimeEndUTC.day).zfill(2)+'_'+str(datetimeEndUTC.hour).zfill(2)+':00:00')

        self._replace_location_strings('namelist.wps')

        #Before running geogrid make a picture of the domain.
        logging.info('Making an image of the domains with plotgrids.ncl')
        util.link_to(self.directory_WPS_input+'/util/plotgrids_new.ncl', directory_WPS_run+'/plotgrids_new.ncl')
        util.replace_string_in_file('plotgrids_new.ncl', 'x11', 'pdf')
        process = subprocess.Popen(['ncl', directory_WPS_run+'/plotgrids_new.ncl'])
        process.wait()


        #Log information to DeepThunder log-file
        logging.info('WPS: run geogrid.exe')
        #Run geogrid.exe
        process = subprocess.Popen([directory_WPS_run+'/geogrid.exe'])
        process.wait()

        if self.inputDataSets.get('ECMWF') is not None:
            os.remove(directory_WPS_run+'/metgrid/METGRID.TBL')
            shutil.copy(self.directory_WPS_input+'/metgrid/METGRID.TBL.ARW', directory_WPS_run+'/metgrid/METGRID.TBL')
            util.replace_string_in_file(directory_WPS_run+'/metgrid/METGRID.TBL', 'name=TT\n        mandatory=yes    # MUST HAVE THIS FIELD', 'name=TT\n        mandatory=yes    # MUST HAVE THIS FIELD\n        derived=yes')
            util.replace_string_in_file(directory_WPS_run+'/metgrid/METGRID.TBL', 'name=UU\n        mandatory=yes    # MUST HAVE THIS FIELD', 'name=UU\n        mandatory=yes    # MUST HAVE THIS FIELD\n        derived=yes')
            util.replace_string_in_file(directory_WPS_run+'/metgrid/METGRID.TBL', 'name=VV\n        mandatory=yes    # MUST HAVE THIS FIELD', 'name=VV\n        mandatory=yes    # MUST HAVE THIS FIELD\n        derived=yes')

        #Log information to DeepThunder log-file
        logging.info('WPS: run metgrid.exe')

        #For each input dataset label
        fg_name = []
        constant = False

        for ids, idso in dsUngrib.iteritems():

            if idso.ungrib_prefix is not None:
                if idso.ungrib_prefix not in fg_name and not idso.is_sst:

                    if idso.type in self.initialConditions or idso.type in self.boundaryConditions:
                        fg_name.append(idso.ungrib_prefix)
                        logging.info('WPS run metgrid.exe chosen to include ' +str(idso.type))
                    else:
                        logging.info('WPS: run metgrid.exe chosen not to run metgrid with '+ str(idso.type))

                elif idso.is_sst and not constant and not self.is_analysis:
                    constant_idso = idso
                    #make sure the date is the same as the date used for ungrib.
                    constant_idso_date = constant_idso.date
                    try:
                        constant_idso_date = constant_idso.get_sst_date()
                    except:
                        logging.error('ERROR: malformed InputDataSet '+ str(idso.type))

                    constant = True
                    logging.info('WPS run metgrid.exe chosen to include ' +str(idso.type) + ' as a constant')
                    util.replace_string_in_file('namelist.wps', '&metgrid\n', '&metgrid\n constants_name = \''+constant_idso.ungrib_prefix+':'+str(constant_idso_date.year)+'-'+str(constant_idso_date.month).zfill(2)+'-'+str(constant_idso_date.day).zfill(2)+'_'+str(constant_idso_date.hour).zfill(2)+'\'\n')  # constants_name = './SST:2015-03-26_00'

                elif idso.is_sst and self.is_analysis and idso.ungrib_prefix not in fg_name:
                    #ungrib the SST.
                    fg_name.append(idso.ungrib_prefix)
                    self.sstintervalseconds = idso.intervalseconds
                    logging.info('WPS run metgrid.exe chosen to include ' +str(idso.type) + ' as a SST')

        #ERAI - add pressure files
        if self.inputDataSets.get('ERAI') is not None:
            fg_name.append('PRES')  #'ERAI','PRES'. That is TWO strings is all we need.

        logging.debug('fg_name is '+str(fg_name))
        util.replace_string_in_file('namelist.wps', ' fg_name = \'FILE\',', ' fg_name = \''+'\', \''.join(fg_name)+'\',')

        #Run metgrid.exe
        logging.info('_run_WPS: metgrid.exe called...')
        process = subprocess.Popen([directory_WPS_run+'/metgrid.exe'])
        process.wait()


    def _ungrib(self, dataType, directory_WPS_run, ungribPrefix, datetimeEndUTC):
        """
        Prepares the namelist.wps file for ungrib.exe and then runs ungrib.exe
        """
        logging.info('_ungrib: ungrib called for '+ str(ungribPrefix)+' dataType is '+str(dataType))
        os.chdir(directory_WPS_run)

        #If the namelist already exists in destination remove it
        if os.path.isfile(directory_WPS_run+'/namelist.wps'):
            os.remove(directory_WPS_run+'/namelist.wps')

        #Copy over a fresh namelist templet
        shutil.copy(self.directory_IBM_input+'/namelist.wps', directory_WPS_run+'/namelist.wps')

        #ungrib with an SST is interesting because the interval time needs to match that of the other datasets. I.e. 6 hours.
        #namelist.wps stores DT_INTERVAL_SECONDS which should already be set.

        if dataType.startswith('SST') and not self.is_analysis:
            logging.debug('_ungrib: using sstdate ' + str(self.datetimeSST))
            util.replace_string_in_file('namelist.wps', 'DT_START_DATE_TIME_DT', str(self.datetimeSST.year)+'-'+str(self.datetimeSST.month).zfill(2)+'-'+str(self.datetimeSST.day).zfill(2)+'_'+str(self.datetimeSST.hour).zfill(2)+':00:00')
        else:
            util.replace_string_in_file('namelist.wps', 'DT_START_DATE_TIME_DT', str(self.datetimeStartUTC.year)+'-'+str(self.datetimeStartUTC.month).zfill(2)+'-'+str(self.datetimeStartUTC.day).zfill(2)+'_'+str(self.datetimeStartUTC.hour).zfill(2)+':00:00')

        util.replace_string_in_file('namelist.wps', 'DT_END_DATE_TIME_DT', str(datetimeEndUTC.year)+'-'+str(datetimeEndUTC.month).zfill(2)+'-'+str(datetimeEndUTC.day).zfill(2)+'_'+str(datetimeEndUTC.hour).zfill(2)+':00:00')

        util.replace_string_in_file('namelist.wps', ' prefix     = \'DT_UNGRIB_PREFIX_DT\'', ' prefix     = \''+ungribPrefix+'\'')
        logging.info('_ungrib: ungrib replacing prefix with ' + str(ungribPrefix))

        self._replace_location_strings('namelist.wps')

        #Link the corresponding Variable table
        logging.debug('_ungrib: link in Vtable assigning Vtable.dataType for '+str(dataType)+' to Vtable.'+str(ungribPrefix))

        #If Vtable already exists remove it. link_to does not overwrite.
        if os.path.isfile('Vtable'):
            os.remove('Vtable')

        util.link_to('ungrib/Variable_Tables/Vtable.'+str(ungribPrefix), 'Vtable')

        #Run linking script
        listOfFileNames = self._get_list_of_inputdatasets(dataType)
        logging.debug('_ungrib: list of files to link based on dataType '+str(dataType)+' is '+str(listOfFileNames))

        listOfFileNames = list(set(listOfFileNames)) # ERAI - Unique grib filenames only.

        logging.info('_ungrib: running link_grib.csh')

        #NOTE we do not sort filenames for erai : UA first, SFC next.
        logging.info('Run link_grib.csh for '.join(listOfFileNames))

        process = subprocess.Popen(['csh', 'link_grib.csh']+listOfFileNames)
        process.wait()

        #Log information to DeepThunder log-file
        logging.info('_ungrib: run ungrib.exe for '+dataType)

        #Setup a log file for ungrib.exe
        ungrib_log = open(str(directory_WPS_run)+'/IBM-CFW-ungrib.log', 'a')

        #Run ungrib.exe
        process = subprocess.Popen([directory_WPS_run+'/ungrib.exe'], stdout=ungrib_log, stderr=ungrib_log)
        process.wait()


    def _get_list_of_inputdatasets(self, dataType):
        """
        Creates a list of all input data set file paths and names
        """

        listOfFileNames = []

        for idso in self.inputfiles:
            if idso.type == dataType:
                if dataType == 'ERAI':
                    listOfFileNames.append(idso.name_prepared.strip())                      # We already have absolute paths. Strip trailing spaces now.
                else:
                    listOfFileNames.append(idso.path+'/'+idso.name_prepared)                # Prefix data dir path to filenames (other than ERAI).

        return listOfFileNames


    def _run_Real(self, directory_Real_run, directory_WPS_run, ids, datetimeEndUTC):
        """
        Prepares and executes real.exe.
        ids = boundary conditions. One dataset only is accepted. This is only used to get
        values for metgrid_levels and metgrid_soil_levels.
        """
        logging.info('_run_Real. real called : '+ str(directory_Real_run) +' : '+ str(directory_WPS_run))
        #Create the run directory for real.exe
        os.makedirs(directory_Real_run)

        shutil.copy(self.directory_IBM_input+'/namelist.input', directory_Real_run+'/namelist.input')

        util.link_to(self.directory_WRF_input+'/real.exe', directory_Real_run+'/real.exe')
        util.link_to(self.directory_WRF_input+'/wrf.exe', directory_Real_run+'/wrf.exe')

        util.link_to(self.directory_WRF_input+'/aerosol.formatted', directory_Real_run+'/aerosol.formatted')
        util.link_to(self.directory_WRF_input+'/aerosol_lat.formatted', directory_Real_run+'/aerosol_lat.formatted')
        util.link_to(self.directory_WRF_input+'/aerosol_lon.formatted', directory_Real_run+'/aerosol_lon.formatted')
        util.link_to(self.directory_WRF_input+'/aerosol_plev.formatted', directory_Real_run+'/aerosol_plev.formatted')
        util.link_to(self.directory_WRF_input+'/CAM_ABS_DATA', directory_Real_run+'/CAM_ABS_DATA')
        util.link_to(self.directory_WRF_input+'/CAM_AEROPT_DATA', directory_Real_run+'/CAM_AEROPT_DATA')
        util.link_to(self.directory_WRF_input+'/CAMtr_volume_mixing_ratio.A1B', directory_Real_run+'/CAMtr_volume_mixing_ratio.A1B')
        util.link_to(self.directory_WRF_input+'/CAMtr_volume_mixing_ratio.A2', directory_Real_run+'/CAMtr_volume_mixing_ratio.A2')
        util.link_to(self.directory_WRF_input+'/CAMtr_volume_mixing_ratio.RCP4.5', directory_Real_run+'/CAMtr_volume_mixing_ratio.RCP4.5')
        util.link_to(self.directory_WRF_input+'/CAMtr_volume_mixing_ratio.RCP6', directory_Real_run+'/CAMtr_volume_mixing_ratio.RCP6')
        util.link_to(self.directory_WRF_input+'/CAMtr_volume_mixing_ratio.RCP8.5', directory_Real_run+'/CAMtr_volume_mixing_ratio.RCP8.5')
        util.link_to(self.directory_WRF_input+'/CLM_ALB_ICE_DFS_DATA', directory_Real_run+'/CLM_ALB_ICE_DFS_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_ALB_ICE_DRC_DATA', directory_Real_run+'/CLM_ALB_ICE_DRC_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_ASM_ICE_DFS_DATA', directory_Real_run+'/CLM_ASM_ICE_DFS_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_ASM_ICE_DRC_DATA', directory_Real_run+'/CLM_ASM_ICE_DRC_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_DRDSDT0_DATA', directory_Real_run+'/CLM_DRDSDT0_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_EXT_ICE_DFS_DATA', directory_Real_run+'/CLM_EXT_ICE_DFS_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_EXT_ICE_DRC_DATA', directory_Real_run+'/CLM_EXT_ICE_DRC_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_KAPPA_DATA', directory_Real_run+'/CLM_KAPPA_DATA')
        util.link_to(self.directory_WRF_input+'/CLM_TAU_DATA', directory_Real_run+'/CLM_TAU_DATA')
        util.link_to(self.directory_WRF_input+'/co2_trans', directory_Real_run+'/co2_trans')
        util.link_to(self.directory_WRF_input+'/ETAMPNEW_DATA', directory_Real_run+'/ETAMPNEW_DATA')
        util.link_to(self.directory_WRF_input+'/ETAMPNEW_DATA_DBL', directory_Real_run+'/ETAMPNEW_DATA_DBL')
        util.link_to(self.directory_WRF_input+'/ETAMPNEW_DATA.expanded_rain', directory_Real_run+'/ETAMPNEW_DATA.expanded_rain')
        util.link_to(self.directory_WRF_input+'/ETAMPNEW_DATA.expanded_rain_DBL', directory_Real_run+'/ETAMPNEW_DATA.expanded_rain_DBL')
        util.link_to(self.directory_WRF_input+'/GENPARM.TBL', directory_Real_run+'/GENPARM.TBL')
        util.link_to(self.directory_WRF_input+'/grib2map.tbl', directory_Real_run+'/grib2map.tbl')
        util.link_to(self.directory_WRF_input+'/gribmap.txt', directory_Real_run+'/gribmap.txt')
        util.link_to(self.directory_WRF_input+'/LANDUSE.TBL', directory_Real_run+'/LANDUSE.TBL')
        util.link_to(self.directory_WRF_input+'/MPTABLE.TBL', directory_Real_run+'/MPTABLE.TBL')
        util.link_to(self.directory_WRF_input+'/ndown.exe', directory_Real_run+'/ndown.exe')
        #The WRF build script has nup.exe commented out with the phrase "#TEMPORARILY REMOVED" with 3.8.1
        util.link_to(self.directory_WRF_input+'/nup.exe', directory_Real_run+'/nup.exe')
        util.link_to(self.directory_WRF_input+'/ozone.formatted', directory_Real_run+'/ozone.formatted')
        util.link_to(self.directory_WRF_input+'/ozone_lat.formatted', directory_Real_run+'/ozone_lat.formatted')
        util.link_to(self.directory_WRF_input+'/ozone_plev.formatted', directory_Real_run+'/ozone_plev.formatted')
        util.link_to(self.directory_WRF_input+'/RRTM_DATA', directory_Real_run+'/RRTM_DATA')
        util.link_to(self.directory_WRF_input+'/RRTM_DATA_DBL', directory_Real_run+'/RRTM_DATA_DB')
        util.link_to(self.directory_WRF_input+'/RRTMG_LW_DATA', directory_Real_run+'/RRTMG_LW_DATA')
        util.link_to(self.directory_WRF_input+'/RRTMG_LW_DATA_DBL', directory_Real_run+'/RRTMG_LW_DATA_DBL')
        util.link_to(self.directory_WRF_input+'/RRTMG_SW_DATA', directory_Real_run+'/RRTMG_SW_DATA')
        util.link_to(self.directory_WRF_input+'/RRTMG_SW_DATA_DBL', directory_Real_run+'/RRTMG_SW_DATA_DBL')
        util.link_to(self.directory_WRF_input+'/SOILPARM.TBL', directory_Real_run+'/SOILPARM.TBL')
        util.link_to(self.directory_WRF_input+'/tc.exe', directory_Real_run+'/tc.exe')
        util.link_to(self.directory_WRF_input+'/tr49t67', directory_Real_run+'/tr49t67')
        util.link_to(self.directory_WRF_input+'/tr49t85', directory_Real_run+'/tr49t85')
        util.link_to(self.directory_WRF_input+'/tr67t85', directory_Real_run+'/tr67t85')
        util.link_to(self.directory_WRF_input+'/URBPARM.TBL', directory_Real_run+'/URBPARM.TBL')
        util.link_to(self.directory_WRF_input+'/URBPARM_UZE.TBL', directory_Real_run+'/URBPARM_UZE.TBL')
        util.link_to(self.directory_WRF_input+'/VEGPARM.TBL', directory_Real_run+'/VEGPARM.TBL')

        os.chdir(directory_Real_run)

        #The name of the first found metgrid file.
        firstMetgridFile = None

        #Create links to the met_em*-files
        for file_name in os.listdir(directory_WPS_run):
            if 'met_em.' in file_name:
                util.link_to(directory_WPS_run+'/'+file_name, directory_Real_run+'/'+file_name)
                #Record the name of the first found metgrid file.
                if firstMetgridFile is None:
                    firstMetgridFile = directory_WPS_run+'/'+file_name


        #Replace place-holders in input file namelist.input
        util.replace_string_in_file('namelist.input', 'DT_RUN_DAYS_DT', '00')

        #For REAL. run hours = max (forecastlength, interval_seconds)
        if self.forecastLength < self.runlength_wps:  # Based on grib input file frequency
            util.replace_string_in_file('namelist.input', 'DT_RUN_HOURS_DT', str(self.runlength_wps).zfill(2))
        else:
            util.replace_string_in_file('namelist.input', 'DT_RUN_HOURS_DT', str(self.forecastLength-self.runshort).zfill(2))

        util.replace_string_in_file('namelist.input', 'DT_RUN_MINUTES_DT', '00')
        util.replace_string_in_file('namelist.input', 'DT_RUN_SECONDS_DT', '00')
        util.replace_string_in_file('namelist.input', 'DT_START_YEAR_DT', str(self.datetimeStartUTC.year))
        util.replace_string_in_file('namelist.input', 'DT_START_MONTH_DT', str(self.datetimeStartUTC.month).zfill(2))
        util.replace_string_in_file('namelist.input', 'DT_START_DAY_DT', str(self.datetimeStartUTC.day).zfill(2))
        util.replace_string_in_file('namelist.input', 'DT_START_HOUR_DT', str(self.datetimeStartUTC.hour).zfill(2))
        util.replace_string_in_file('namelist.input', 'DT_START_MINUTES_DT', '00')
        util.replace_string_in_file('namelist.input', 'DT_START_SECONDS_DT', '00')
        util.replace_string_in_file('namelist.input', 'DT_END_YEAR_DT', str(datetimeEndUTC.year))
        util.replace_string_in_file('namelist.input', 'DT_END_MONTH_DT', str(datetimeEndUTC.month).zfill(2))
        util.replace_string_in_file('namelist.input', 'DT_END_DAY_DT', str(datetimeEndUTC.day).zfill(2))
        util.replace_string_in_file('namelist.input', 'DT_END_HOUR_DT', str(datetimeEndUTC.hour).zfill(2))
        util.replace_string_in_file('namelist.input', 'DT_END_MINUTES_DT', '00')
        util.replace_string_in_file('namelist.input', 'DT_END_SECONDS_DT', '00')
        util.replace_string_in_file('namelist.input', 'DT_MAX_DOM_DT', str(max(self.domains)))
        util.replace_string_in_file('namelist.input', 'DT_INTERVAL_SECONDS', str(self.WPSintervalseconds))

        for dom in range(len(self.domain_dims_dx)):
            util.replace_string_in_file('namelist.input', 'DT_DX_'+str(dom+1)+'_DT', str(self.domain_dims_dx[dom]))
            util.replace_string_in_file('namelist.input', 'DT_DY_'+str(dom+1)+'_DT', str(self.domain_dims_dy[dom]))
            #history_interval
            util.replace_string_in_file('namelist.input', 'DT_HIST_'+str(dom+1)+'_DT', str(self.domain_history_interval[dom]))

        for dom in range(len(self.domain_dims_dx), self.MAXINSTRUMENTEDDOMAINS+1):
            util.replace_string_in_file('namelist.input', 'DT_DX_'+str(dom+1)+'_DT', str(1))
            util.replace_string_in_file('namelist.input', 'DT_DY_'+str(dom+1)+'_DT', str(1))
            #history_interval
            util.replace_string_in_file('namelist.input', 'DT_HIST_'+str(dom+1)+'_DT', str(self.DEFAULT_HIST_INT))

        util.replace_string_in_file('namelist.input', 'DT_PARENT_GRID_RATIO_DT', str(self.parent_grid_ratio))



        #Setting default metgrid levels based on those from GFS. Will overwrite.
        DT_NUM_METGRID_LEVELS_DT = '27'
        DT_NUM_METGRID_SOIL_LEVELS_DT = '4'

        #If we have found at least one metgrid file then:
        if firstMetgridFile is not None:
            #Set DT_NUM_METGRID_LEVELS_DT and DT_NUM_METGRID_SOIL_LEVELS_DT based on those found in the metgrid file.
            DT_NUM_METGRID_LEVELS_DT = str(self._get_num_metgrid_levels(firstMetgridFile))
            DT_NUM_METGRID_SOIL_LEVELS_DT = str(self._get_num_metgrid_soil_levels(firstMetgridFile))
            logging.debug('_run_Real setting metgrid levels and soil levels to '+ str(DT_NUM_METGRID_LEVELS_DT)+' '+ str(DT_NUM_METGRID_SOIL_LEVELS_DT))
        else:
            logging.error('_run_Real NO metgrid files found. This will be fatal')

        #Replace place-holders in input file namelist.input for the number of levels
        util.replace_string_in_file('namelist.input', 'DT_NUM_METGRID_LEVELS_DT', DT_NUM_METGRID_LEVELS_DT)
        util.replace_string_in_file('namelist.input', 'DT_NUM_METGRID_SOIL_LEVELS_DT', DT_NUM_METGRID_SOIL_LEVELS_DT)
        util.replace_string_in_file('namelist.input', 'DT_TIME_STEP_DT', str(self.timeStepForecast))
        util.replace_string_in_file('namelist.input', 'DT_VERT_COUNT_DT', str(self.num_vertical_levels))

        #PHYSICS options start
        util.replace_string_in_file('namelist.input', 'DT_MPPH', str(self.phys_mp_val))
        util.replace_string_in_file('namelist.input', 'DT_RALWPH', str(self.phys_ralw_val))
        util.replace_string_in_file('namelist.input', 'DT_RASWPH', str(self.phys_rasw_val))
        util.replace_string_in_file('namelist.input', 'DT_SFC', str(self.phys_sfcc_val))
        util.replace_string_in_file('namelist.input', 'DT_SUR', str(self.phys_sfc_val))
        util.replace_string_in_file('namelist.input', 'DT_PBLPH', str(self.phys_pbl_val))
        for i in self.domains:
            custr = 'DT_CUPH' +str(i)
            util.replace_string_in_file('namelist.input', custr, str(self.phys_cu_val[i-1]))
        for i in self.idomains:
            if i > max(self.domains):
                custr = 'DT_CUPH' +str(i)
                util.replace_string_in_file('namelist.input', custr, str(0))
        util.replace_string_in_file('namelist.input', 'DT_URB', str(self.phys_urb_val))
        #PHYSICS options end

        self._replace_location_strings('namelist.input')

        #Disable or enable auxhist2 and auxhist7
        if self.auxhist7:
            #Enable in hours
            util.replace_string_in_file('namelist.input', 'DT_AUX7', str(1))
        else:
            #Set to zero to disable.
            util.replace_string_in_file('namelist.input', 'DT_AUX7', str(0))

        if self.auxhist2:
            #Enable (in minutes)
            util.replace_string_in_file('namelist.input', 'DT_AUX2', str(60))
        else:
            #Disable
            util.replace_string_in_file('namelist.input', 'DT_AUX2', str(0))

        #Disable or enable feedback
        if self.feedback:
            #If feedback is on set to 1
            util.replace_string_in_file('namelist.input', 'DT_FEEDBACK', str(1))
        else:
            #Otherwise set to 0
            util.replace_string_in_file('namelist.input', 'DT_FEEDBACK', str(0))

        #Disable or enable adaptive time steps
        if self.adaptivets:
            #If feedback is on set to 1
            util.replace_string_in_file('namelist.input', 'DT_ADTS', '.true.')
        else:
            #Otherwise set to 0
            util.replace_string_in_file('namelist.input', 'DT_ADTS', '.false.')

        #Setup SST_UPDATE
        #Two flags need changing auxinput4_interval to the minutes between updates and sst_update to 1.
        #For SST Updates to work aux input 4 files io_form_auxinput4 and auxinput4_inname must be set.
        DT_AUX4_INT_DT = (self.sstintervalseconds/60)

        #If the reanalysis flag is on turn SST updates on.
        if self.is_analysis:
            DT_SST_UPDATE_DT = 1
        else:
            DT_SST_UPDATE_DT = 0

        util.replace_string_in_file('namelist.input', 'DT_AUX4_INT_DT', str(DT_AUX4_INT_DT))
        util.replace_string_in_file('namelist.input', 'DT_SST_UPDATE_DT', str(DT_SST_UPDATE_DT))


        #No obsnudging in Stevedore (this will change in the future)
        DT_AUX11 = 0
        DT_AUXEH11 = 0
        util.replace_string_in_file('namelist.input', 'DT_AUX11', str(DT_AUX11))
        util.replace_string_in_file('namelist.input', 'DT_AUXEH11', str(DT_AUXEH11))

        # Run real.exe with -np processes
        try:
            process = subprocess.Popen(['mpirun', '-np', str(self.numberCores), './real.exe'])
            process.wait()
        except OSError as os_err:
            logging.error('real.exe log-file failed to run. Please check that mpirun is in your path.')
            logging.error(' error is '+ str(os_err.strerror))


        #Log information to log-file
        logging.info('real.exe log-file')

        #Copy real.exe log into general stevedore log-file
        reallog = open(directory_Real_run+'/rsl.error.0000').read()
        logging.info(reallog)

        #run_hours for WRF set to forecast length here
        util.replace_string_in_file('namelist.input', 'DT_RUN_HOURS_DT', str(self.forecastLength))


    def run_WRF(self):
        """
        Runs WRF. After copying the required files it runs real.exe and then starts wrf.exe.
        """

        #Log information to DeepThunder log-file
        logging.info('Run WRF ...')

        if self.norunwrf:
            #No need to create directories not running wrf.
            logging.info('No need to create directories for wrf output, we are not running wrf')
        else:
            # Create directories for wrf output
            if os.path.exists(self.directory_wrf_run):
                logging.info('run_WRF: Moving old wrf run to wrf_old ...')
                if os.path.exists(self.directory_wrf_run+'_old'):
                    logging.info('run_WRF: removing old wrf_old directory ...')
                    shutil.rmtree(self.directory_wrf_run+'_old')
                shutil.move(self.directory_wrf_run, self.directory_wrf_run+'_old')

        if not self.norunwrf:
            shutil.copytree(self.directory_PreProcessing_run+'/Real_boundary', self.directory_wrf_run)

            if os.path.exists(self.directory_PreProcessing_run+'/Real_initial'):
                for domain in self.domains:
                    os.remove(self.directory_wrf_run+'/wrfinput_d0'+str(domain))
                    shutil.copyfile(self.directory_PreProcessing_run+'/Real_initial/wrfinput_d0'+str(domain), self.directory_wrf_run+'/wrfinput_d0'+str(domain))

        #Hook in Tslist if this file exists in then read copy it to the run directory.
        if self.tsfile is not None:
            logging.info('run_WRF: a time series file (ts_file) is being read in from '+ str(self.tsfile))
            if os.path.exists(self.tsfile):
                shutil.copyfile(self.tsfile, self.directory_wrf_run+'/tslist')

        #Run WRF
        self._execute_WRF(self.numberCores, self.directory_wrf_run, self.norunwrf)

        #Log end-of-run
        logging.info('run_WRF: wrf.exe finished!')


    @staticmethod
    def _execute_WRF(number_cores, directory_wrf_run, norunwrf):
        """
        Starting wrf.exe
        """
        if norunwrf:
            logging.info('_execute_WRF: run WRF.exe has been skipped by the user. ')
        else:
            # record start of execution of wrf.exe
            logging.info('_execute_WRF: run WRF.exe')
            os.chdir(directory_wrf_run)
            process = subprocess.Popen(['mpirun', '-np', str(number_cores), './wrf.exe'])
            process.wait()


    @staticmethod
    def _download_geog_data(directory_root_geog, directory_root_input):
        """
        Download static geog data if the user does not already have it.
        """
        logging.warning('_download_geog_data no geog data exists. This will be downloaded for you and extracted to '+directory_root_geog)
        os.makedirs(directory_root_geog)

        process = subprocess.Popen(['/bin/bash', '/opt/deepthunder/stevedore/scripts/download_geog_data.sh'])
        process.wait()

        os.chdir(directory_root_input)

    @staticmethod
    def _get_num_metgrid_levels(filename):
        """
        From a netcdf file output from metgrid obtain the value of num_metgrid_levels
        """
        #ncdump -h met_em.d01.2017-04-12_00:00:00.nc | grep 'num_metgrid_levels =' | cut -d " " -f3
        #log entry:
        logging.debug("In _get_num_metgrid_levels")

        #Load the dataset from the selected filename.
        froot = Dataset(filename, "r", format="NETCDF4")

        #Get the dimension 'num_metgrid_levels'
        levels = froot.dimensions["num_metgrid_levels"]

        #Return the size of num_metgrid_levels as an int.
        return int(len(levels))


    @staticmethod
    def _get_num_metgrid_soil_levels(filename):
        """
        From a netcdf file output from metgrid obtain the value of num_metgrid_soil_levels
        """
        #log entry:
        logging.debug("In _get_num_metgrid_soil_levels")

        #Load the dataset from the selected filename.
        rootgrp = Dataset(filename, "r", format="NETCDF4")

        #get the attributes
        soil_levels = getattr(rootgrp, 'NUM_METGRID_SOIL_LEVELS')

        #Return the number of soil levels as an int.
        return int(soil_levels)
