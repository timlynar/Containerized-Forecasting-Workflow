#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
IBM Containerized Forecasting Workflow

DESCRIPTION

    sanity checks

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
from multiprocessing import Process, Queue, cpu_count

def is_sane(stevedore_inst):
    """
    runs some simple sanity checks before execution.
    Checks focus on expected diskspace usage vs availability and common config issues.
    """
    logging.info("_is_sane begin checks")

    sane = True

    #Is the user likely to run out of disk space?
    #Provided the user uses all default options then the output size is about 159.13 bytes per point.
    #Is addition you need to add the estimated disk space for input data.
    bytes_per_point = 159.13
    #GFS is about 70M every 3 hours so lets say about 23M per hour.
    bytes_per_hour_input = 24000000
    estimate_usage = 0
    for d in stevedore_inst.domains:
        domain_points = stevedore_inst.domain_dims_nx[d-1] * stevedore_inst.domain_dims_ny[d-1] * stevedore_inst.num_vertical_levels
        estimate_usage = estimate_usage+domain_points

    #The estimated usage in bytes is is produced data + downloaded data.
    estimate_usage = (domain_points*bytes_per_point*stevedore_inst.forecastLength)+(bytes_per_hour_input * stevedore_inst.forecastLength)

    #Get the free space available
    freedisk = os.statvfs(stevedore_inst.directory_root_run)
    freediskbytes = freedisk.f_bavail*freedisk.f_frsize
    msg = "estimated disk usage for the run is "+str(estimate_usage) +" Bytes which is " + str(estimate_usage/1024/1024) + " MiB"
    if freediskbytes < estimate_usage:
        logging.warning("_is_sane "+msg+" is greater than free space which is "+str(freediskbytes/1024/1024)+" MiB")
        sane = False

    #Are we using more cores than the system has?
    cores = cpu_count()
    if cores < stevedore_inst.numberCores:
        logging.warning("_is_sane you have requested the use of more cores than this node has. Requested  "+str(stevedore_inst.numberCores)+" have "+str(cores))
        sane = False

    #The whole e_we, e_sn, ratio thing.
    #(e_we-s_we+1) must be one greater than an integer multiple of the parent_grid_ratio
    for d in stevedore_inst.domains:
        if (stevedore_inst.domain_dims_nx[d-1] % stevedore_inst.parent_grid_ratio) != 1:
            logging.warning("_is_sane domain "+str(d)+ " nx "+ str(stevedore_inst.domain_dims_nx[d-1])+ " ny "+ str(stevedore_inst.domain_dims_ny[d-1]) + " ratio "+ str(stevedore_inst.parent_grid_ratio) + " this is not sane and needs correcting")
            sane = False

    #If we are not sane then log that.
    if not sane:
        logging.warning("_is_sane returns " + str(sane) + ' no corrections have been made. This is just a warning.')

    return sane
