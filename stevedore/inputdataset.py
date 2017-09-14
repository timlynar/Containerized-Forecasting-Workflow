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
import subprocess
import shutil


class InputDataSet(object):
    '''
    A class defining a input data set.
    '''
    # pylint: disable=too-many-instance-attributes

    #where to put the rda login file.
    RDA_LOGIN_PATH = '/opt/deepthunder/data'

    #Where to store observation data.
    DIRECTORY_ROOT_OBSERVATIONS = '/opt/deepthunder/data/observations'

    def __init__(self, date, hour, path, **args):
        '''
        Constructor of a InputDataSet object.
        '''
        self.type = None
        self.date = date
        self.hour = hour
        #sub-directory we want to store the data in
        self.path = path
        #File name.
        self.name = None
        #The filename after it is processed for ingest
        self.name_prepared = None
        #server url i.e. ftp://blah.com
        self.server_url = []
        #Where on the server can we get the file i.e. the path
        self.server_path = []
        #This is an alternate server to use first (set at runtime)
        self.alt_server_url = None
        #Do you want to overwrite the file.
        self.keep_existing_file = True
        #Do you want to ungrib the data or just download it
        self.ungrib = True
        #interval_seconds (namelist.wps parameter). Default 3 hours
        self.intervalseconds = 10800

        #does this dataset use RDA.
        self.is_rda = []
        #is this a historical run but not a hindcast
        self.is_analysis = False
        #is this a SST
        self.is_sst = False
        #The prefix used by ungrib.exe as specified in namelist.wps
        self.ungrib_prefix = 'NONE'
        #The server to use, when there are multiple.
        self.server_pos = 0


    def download(self):
        '''
        Download a given file for a given dataset based on the time and date.
        '''
        #Flag to indicate if it has been downloaded or not.
        downloaded = False
        #Set the initial source to 0.
        data_source = -1
        #log download has been called.
        logging.debug('download called for '+ self.name)
        #If the file already exists and we are keeping existing files do not download it.
        if self.keep_existing_file and self.exists():
            logging.info('existing file found  '+ self.name + ' will not download.')

        #Otherwise begin the download process.
        else:
            #Log what we are downloading
            logging.debug(str(current_process().name)+', file name:' +self.name)

            # Create directories if they do not exist.
            if not os.path.exists(self.path):
                os.mkdir(self.path)

            #Change to the new data directory.
            os.chdir(self.path)

            # Delete input data set file if it exists
            if os.path.isfile(self.path+'/'+ self.name):
                os.remove(self.path+'/'+ self.name)

            # Delete prepared input data set file if it exists
            if os.path.isfile(self.path+'/'+ self.name_prepared):
                os.remove(self.path+'/'+ self.name_prepared)

            #IF you specify that you have an alternate server then we will use
            # it by appending it to the start of the server list
            if self.alt_server_url != None:
                self.is_rda = [False] + self.is_rda
                #append the alternate server to the start of the list.
                self.server_url = [self.alt_server_url] + self.server_url
                logging.info('Alt Server location set as  '+ str(self.alt_server_url))
                self.server_path = ['pub/'+self.type] + self.server_path
                logging.info('Setting path to  '+ str('pub/'+self.type))


            while not downloaded and data_source < (len(self.server_url) -1):
                #increment the data_source to use.
                data_source = data_source + 1
                self.server_pos = data_source

                #Try to download the file.
                try:
                    #construct the full url to the file to download.
                    full_url = self.server_url[data_source]+'/'+\
                              self.server_path[data_source]+'/'+self.name

                    #if the file is an RDA file.
                    if self.is_rda[data_source]:
                        #login / check if we need to login.
                        self.rda_login()
                        #Download the file passing in the cookie.
                        process = subprocess.Popen(['wget', '--load-cookies',
                                                    self.RDA_LOGIN_PATH+'/auth.rda.ucar.edu.$$',
                                                    full_url,
                                                    '--output-document='+self.path+'/'+self.name])
                        process.wait()
                    else:
                        #Try to download the file.
                        #Waiting 17 seconds between tries and trying 5 times.
                        process = subprocess.Popen(['wget', full_url,
                                                    '--output-document='\
                                                    +self.path+'/'+self.name,
                                                    '--retry-connrefused',
                                                    '--waitretry=17', '-t 5'])
                        process.wait()

                    #Log that we have downloaded the file.
                    logging.info('Downloaded '+full_url)

                    if process.returncode != 0:
                        logging.info('Failure downloading '+ full_url +' retrying ...')
                    else:
                        # check integrity of the file with wgrib2.
                        # This assumes it is a grib file
                        try:
                            process = subprocess.Popen(['wgrib2', self.path+'/'+self.name])
                            process.wait()
                            if process.returncode != 0:
                                logging.info('Grib file appears corrupt.'
                                             ' Retrying download '+ full_url)
                            else:
                                downloaded = True
                        except OSError:
                            logging.error('OSError File downloaded but does not exist or wgrib2 is failing.')


                except ftplib.error_perm, resp:
                    logging.info('Current process:'+str(current_process().name)+\
                                 ' Exception: '+str(resp))
                    downloaded = False


    def rda_login(self):
        """
        Login to rda.ucar.edu using the credentials found in RDA_EMAIL and RDA_PASS
        """
        loginfile = self.RDA_LOGIN_PATH+'/login'
        logging.info('looking for '+ loginfile)

        #set not logged in to True.
        notloggedin = True

        # Check that the login file is not too old.
        if os.path.isfile(loginfile):
            mtime = os.path.getmtime(loginfile)
            #If the login file was modified more than an hour ago remove it and login again.
            #Note: assume login once every hour is ok.
            modtime = int(time.time()) - int(mtime)
            if modtime > 3600:
                logging.info('Removing old login file with modification time of '+ str(modtime))
                os.remove(loginfile)

            notloggedin = not os.path.isfile(loginfile)

        #after defineing notloggedin check its value
        if notloggedin:
            logging.info('Login to auth.rda.ucar.edu with credentials'
                         'supplied by environment variables RDA_EMAIL and RDA_PASS')
            try:
                if os.environ.get('RDA_EMAIL') is None:
                    print "SET the environment variables RDA_EMAIL and RDA_PASS "
                else:
                    process = subprocess.Popen(['wget', '--save-cookies',
                                                'auth.rda.ucar.edu.$$',
                                                '--post-data="email='\
                                                +os.environ.get('RDA_EMAIL')\
                                                +'&passwd='+os.environ.get('RDA_PASS')\
                                                +'&action=login"',
                                                'https://rda.ucar.edu/cgi-bin/login'])
                    process.wait()

                    #Copy the login file and cookie out of here:
                    if os.path.isfile(self.path+'/login'):
                        shutil.copyfile(self.path+'/login', loginfile)

                    if os.path.isfile(self.path+'/auth.rda.ucar.edu.$$'):
                        logging.debug('copy cookie')
                        shutil.copyfile(self.path+'/auth.rda.ucar.edu.$$',
                                        self.RDA_LOGIN_PATH+'/auth.rda.ucar.edu.$$')
            except:
                logging.error("SET the environment variables RDA_EMAIL and"
                              " RDA_PASS to use this dataset")

        else:
            logging.info('Already logged in to RDA. Ready to download data...')
            logging.info('Downloading ' +self.name +\
                         ' from: http://'+self.server_url[self.server_pos]+\
                         '/'+self.server_path[self.server_pos])

    def exists(self):
        """
        Does the file exist in either pre or post processed forms? if so return True.
        """
        does_exist = False

        if os.path.isfile(self.path+'/'+self.name) and \
           os.path.getsize(self.path+'/'+self.name) > 0:
            does_exist = True

        if os.path.isfile(self.path+'/'+self.name_prepared) and \
           os.path.getsize(self.path+'/'+self.name_prepared) > 0:
            does_exist = True

        return does_exist


    def prepare(self, **args):
        '''
        Steps to transform the downloaded input data into the files needed
        by WPS or by other functions as required
        '''

        logging.debug('Running prepare for ' + str(self.type) + ' with arguments ' + str(args))

        return
