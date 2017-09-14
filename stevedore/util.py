#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
IBM Containerized Forecasting Workflow - utility funtion module.

DESCRIPTION

generic utility functions.

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

def link_to(src, dst):
    """
    Create a symlink.  If the link already exists record the error.
    """
    try:
        #Try to create the symlink
        os.symlink(src, dst)
    except OSError, os_err:
        #If the symlink errors then record that error
        logging.error("Could not make link to "+str(dst)+ " from "+ str(src) +
                      " reason: "+ str(os_err))


def convert_to_list(var, max_domains, default_value_in='0'):
    """
    Convert a variable to a list the length of the number of domains.
    """
    #how many domains
    ndomains = max_domains
    #The output variable = the input
    varlist = var
    #if the input is already a list
    if isinstance(var, list):
        nitems = len(var)
        if nitems == 1:
            varlist = var*ndomains
        elif len(var) != ndomains:
            #Complain first
            logging.error("array length is greater than 1 but does not match "+
                          "number of domains for variable ")
            #no point just complaining about it. Will make it work.
            while len(varlist) < ndomains:
                varlist.append(str(default_value_in))

    else:
        varlist = [var]*ndomains
    return varlist


def replace_string_in_file(file_name, string_to_replace, string_to_insert):
    """
    Replace all the string_to_replace with the string_to_insert in file file_name.
    """

    logging.debug('_replace_string_in_file: File '+file_name + ' '+
                  string_to_replace + ' to '+ string_to_insert)

    string = open(file_name).read()
    string = string.replace(string_to_replace, string_to_insert)
    textfile = open(file_name, 'w')
    textfile.write(string)
    textfile.close()
