#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
DESCRIPTION
    Basic run script for IBM Containerized Forecasting Workflow

EXAMPLES

    This script can be called with the following command line:

    python run_simulation.py --start 2014-08-02 --end 2014-08-04 --length 48 \
    --lat 53.343 --long -6.266 --hour 0

    This runs 3 different 48 (--length) hour forecasts for the days between
    2.8.2014 (--start) and 4.8.2014 (--end) with the domains centred at latitude
    53.343 (--lat) and longitude -6.266 (--lon). The forecasts are starting at
    hour 0 (midnight) (---hour)

AUTHOR

    Tim Lynar <timlynar@au1.ibm.com>, IBM Research, Melbourne, Australia
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

import sys
import argparse
from datetime import datetime, timedelta as td
import stevedore
from stevedore.sanity import is_sane

def main(argv):
    '''
    Main - run a simulation
    '''

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', help="Start date and time in format"
                                            " yyyy-mm-dd")

        parser.add_argument('--end', help="End date and time in format"
                                          " yyyy-mm-dd")

        parser.add_argument('--length', help="Forecast length in hours")

        parser.add_argument('--lat', help="Latitude in format 99.99 Can specify"
                                          "single value for all, or exactly one"
                                          "per domain starting with domain 1",
                            type=float, nargs='+')

        parser.add_argument('--long', help="Longitude in format 99.99 you Can"
                                           "specify single value for all, or "
                                           "exactly one per domain starting "
                                           "with domain 1",
                            type=float, nargs='+')

        parser.add_argument('--hour', help="Start hour", default=0)

        parser.add_argument('--ngridew', help="Grid count for domains in ew."
                                              " Can specify single value for"
                                              "all, or exactly one per domain",
                            default=100, nargs='+', type=int)

        parser.add_argument('--ngridns', help="Grid count for domains in ns."
                                              " Can specify single value for"
                                              " all, or exactly one per domain",
                            default=100, nargs='+', type=int)

        parser.add_argument('--nvertlevels', help="Number of vertical levels",
                            type=int, default=40)

        parser.add_argument('--ndomains', help="Number of domains", type=int,
                            default=3)

        parser.add_argument('--gridratio', help="Nest refinement "
                                                "ratio for domains", default=3)

        parser.add_argument('--gridspacinginner', help=" Single value for dx "
                            "and dy of inner domain, in km (others will be"
                            " calculated based on ratio)", default=1.5)

        parser.add_argument('--timestep', help=" Timestep (minutes)",
                            default=10)

        parser.add_argument('--wpsmapproj', help=" WPS map projection",
                            default='lambert')

        parser.add_argument('--sitefile', help=" Custom namelist path for WPS"
                                               " and WRF this path must have "
                                               "two files under it "
                                               "namelist.wps and"
                                               " namelist.input",
                            default=None)

        parser.add_argument('--tslistfile', help="Activate time series output. "
                                                 "point to a tslist file see "
                                                 "README.tslist for more info",
                            default=None)

        parser.add_argument('--ncores', help=" Define number of cores",
                            type=int, default=2)

        parser.add_argument('--history_interval', type=int, help='history output file interval in minutes',
                            default=[60], nargs='*', dest='history_interval')

        #Arguments for physics options.
        #For options see:
        #http://www2.mmm.ucar.edu/wrf/users/docs/user_guide_V3/users_guide_chap5.htm

        #Note that these options are executed the same for each domain.
        # If you need something else edit the namelist.
        #mp_physics #Micro Physics (Double or single moment) (1=kessler,
        # 2=Purdue Lin et al, 3=WSM 3-class, 4=WSM 4-class,  5=Ferrier, 6=WSM 6-class,
        # 8=New Thompson, 9=Milbrandt-yau 2-moment, 10=Morrison 2-moment,
        # 13=Stonybrook (SBU), 14=WDM 5-class, 17=NSSL-mom, 16=WDM 6 class,
        # 17=Goddard 6-class, 93=GD,  98=old Thompson )
        parser.add_argument('--phys_mp', help=" Micro Physics (double or "
                                              "single moment)",
                            default=17, type=int,
                            choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14,
                                     16, 17, 18, 19, 21, 22, 28, 30, 32, 93, 98])
        #Radiation Long wave Physics
        #ra_lw_physics(1=RRTM, 3=CAM3, 4=RRTMG, 5=goddard, 31=Held-Suarez, 99=GFDL)
        parser.add_argument('--phys_ralw', help=" Radiation long wave physics,"
                                                " (1=RRTM, 3=CAM3, 4=RRTMG, "
                                                "5=goddard, 31=Held-Suarez, "
                                                "99=GFDL)",
                            default=4, type=int,
                            choices=[1, 2, 3, 4, 5, 31, 99])

        #ra_sw_physics #Radiation Short wave Physics (1=mm5 Dudhia, 2=Goddard,
        # 3=cam3, 4=RRTMG, 5=New Goddard SW, 99=GFDL scheme)
        parser.add_argument('--phys_rasw', help=" Radiation short wave physics",
                            default=4, type=int,
                            choices=[1, 2, 3, 4, 5, 99])

        #cu_physics #Cumulus scheme (1=new Kain-Fritsch, 2=Betts-miller-Janjic,
        # 3=Grell-Devenyi Ensemble, 4=SAS, 5=Grell-3d, 6=Tiedtke,
        #7=CAM Zhang-McFarlane, 14=NSAS, 99=Old Kain-Fritsch)
        parser.add_argument('--phys_cu', help=" Cumulus scheme, can specify"
                                              " only one or exactly one for"
                                              " each domain",
                            default=0, nargs='+', type=int,
                            choices=[0, 1, 2, 3, 4, 5, 6, 7, 14, 99])

        #bl_pbl_physics #Planetary boundary layer (1=YSU, 2=Mellor-Yamada-janjic,
        # 3=Eta, 4=QNSE, 5=MYNN, 6=MYNN, 7=(ACM2) Asymmetrical convective model 2,
        # 8=Boulac, 9=CAM UW, 10=TEMF, 99=MRF)

        parser.add_argument('--phys_pbl', help=" Planetary boundary layer",
                            default=1, type=int,
                            choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 99])

        #sf_sfclay_physics
        parser.add_argument('--phys_sfcc', help=" Surface clay physics",
                            default=1, type=int)
        #sf_surface_physics
        parser.add_argument('--phys_sfc', help=" Surface physics",
                            default=2, type=int)
        #sf_urban_physics (0 = off 1-3 = on,)
        parser.add_argument('--phys_urb', help=" Urban physics",
                            default=0, type=int, choices=[0, 1, 2, 3])
        #end of physics options
        parser.add_argument('--runshort', help=" Number of extra hours to prep"
                                               " but not execute",
                            default=0, type=int, choices=[0, 1])
        parser.add_argument('--auxhist7', help=" Turn on auxhist7"
                                               " output to hourly",
                            dest='auxhist7', action='store_true')
        parser.add_argument('--auxhist2', help=" Turn on auxhist2"
                                               " output to hourly",
                            dest='auxhist2', action='store_true')
        parser.add_argument('--feedback', help=" Turn feedback on",
                            dest='feedback', action='store_true')
        parser.add_argument('--adaptivets', help=" Turn adaptive time steps on",
                            dest='adaptivets', action='store_true')
        parser.add_argument('--no-preprocess', help=" Turn pre-processing off",
                            dest='nopreprocess', action='store_true')
        parser.add_argument('--projectdir', help=" The project sub-directory",
                            dest='projectdir', default="undefined")
        parser.add_argument('--no-wrf', help=" Turn wrf.exe execution off",
                            dest='norunwrf', action='store_true')
        parser.add_argument('--is-Analysis', help=" Perform a long run "
                                                  "historical simulation",
                            dest='is_analysis', action='store_true')
        #alt ftp server
        parser.add_argument('--altftpserver', help=" Alternate data server. "
                                                   "if this is set then all "
                                                   "data will be downloaded "
                                                   "from this ftp server rather"
                                                   " than those preset",
                            default=None, dest='altftpserver')
        #data to use
        parser.add_argument('--initialConditions', help=" Data to use for "
                                                        "initial conditions "
                                                        "(see README.md for "
                                                        "options) ",
                            default=['GFS'], dest='initialConditions',
                            nargs='+')

        parser.add_argument('--boundaryConditions', help=" Data to use for "
                                                         "boundary conditions "
                                                         "(see README.md for "
                                                         "options) ",
                            default=['GFS'], dest='boundaryConditions',
                            nargs='+')

        parser.add_argument('--inputData', help=" Input data for IC and LBCs -"
                                                " (see README.md for options) ",
                            default=[], nargs='+',
                            dest='inputData')

        args = parser.parse_args()

    except Exception, general_exception:
        print 'Exception '+ str(general_exception)

    # create date objects
    hour_start = int(args.hour)
    #get the start date expecting it to be in the format YYYY-MM-DD
    date_start = datetime.strptime(str(args.start)+':'+str(hour_start),
                                   '%Y-%m-%d:%H')
    #get the end date expecting it to be in the format YYYY-MM-DD
    date_end = datetime.strptime(str(args.end)+':'+str(hour_start),
                                 '%Y-%m-%d:%H')
    #calculate the difference between the two dates.
    date_delta = date_end - date_start
    forecast_length = int(args.length)

    #Note print off what is going on to aid in debugging
    print "Running "+ str(date_delta.days+1) +" individual simulation each being "\
    +str(forecast_length) +" hours in length. UTC start is "+ str(hour_start)

    # loop over days to create forecasts
    for i in range(date_delta.days + 1):

        # current date of loop
        date_i = date_start + td(days=i)

        stevedore_instance = stevedore.Stevedore(date_i,
                                       forecast_length, args.lat, args.long,
                                       ncores=int(args.ncores),
                                       ndomains=int(args.ndomains),
                                       timestep=int(args.timestep),
                                       gridratio=int(args.gridratio),
                                       gridspacinginner=float(args.gridspacinginner),
                                       ngridew=args.ngridew,
                                       ngridns=args.ngridns,
                                       nvertlevels=int(args.nvertlevels),
                                       phys_mp=int(args.phys_mp),
                                       phys_ralw=int(args.phys_ralw),
                                       phys_rasw=int(args.phys_rasw),
                                       phys_cu=args.phys_cu,
                                       phys_pbl=int(args.phys_pbl),
                                       phys_sfcc=int(args.phys_sfcc),
                                       phys_sfc=int(args.phys_sfc),
                                       phys_urb=int(args.phys_urb),
                                       wps_map_proj=args.wpsmapproj,
                                       runshort=int(args.runshort),
                                       auxhist7=args.auxhist7,
                                       auxhist2=args.auxhist2,
                                       feedback=args.feedback,
                                       adaptivets=args.adaptivets,
                                       projectdir=args.projectdir,
                                       norunwrf=args.norunwrf,
                                       is_analysis=args.is_analysis,
                                       altftpserver=args.altftpserver,
                                       initialConditions=args.initialConditions,
                                       boundaryConditions=args.boundaryConditions,
                                       inputData=args.inputData,
                                       tsfile=args.tslistfile,
                                       history_interval=args.history_interval)


        # check if the object is sane.
        #Check for sanity
        is_sane(stevedore_instance)

        # check if input data sets already exist, if not download it.
        if not args.nopreprocess:
            stevedore_instance.check_input_data()

            # run pre-processing System
            stevedore_instance.run_preprocessing()

        # run WRF
        stevedore_instance.run_WRF()


    print 'Stevedore has finished'

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
        sys.exit(0)

    except KeyboardInterrupt, kb_exception: # Ctrl-C
        raise kb_exception

    except SystemExit, system_exception: # sys.exit()
        raise system_exception

    except Exception, general_exception:
        print str(general_exception)
        sys.exit(1)
