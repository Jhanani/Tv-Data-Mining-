# -*- coding: utf-8 -*-

# For command-line arguments
import sys, getopt

# For HTTP requests
from urllib2 import Request, urlopen, HTTPError

# For time formatting
#import datetime
import time
from datetime import datetime, timedelta, date
from dateutil.parser import parse

# For json data
import json

# For faster file read
import MySQLdb
# For faster file read
import pandas as pd
import numpy as np

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Import the declarative models
import fwm_config as fwm_config

# Configuration containing the DB name and login credentials
config = fwm_config.Config()

# Constants
months_with_30_days = ['Apr', 'Jun', 'Sep', 'Nov']
months_with_31_days = ['Jan', 'Mar', 'May', 'Jul', 'Aug', 'Oct', 'Dec']

# Add viewtime information
def add_viewtime_info(records, tms_id_prog_name_dict):
    print 'Add viewtme and program info'

    ref_record = records[0]
    ref_datetime = datetime.combine(ref_record['event_date'], datetime.min.time()) + ref_record['event_time']
    program_name = None
    try:
        program_name = tms_id_prog_name_dict[ref_record['program_tms_id']]
    except:
        program_name = None
    ref_record['program_name'] = program_name
    for item in records[1:]:
        # Check if the device id is same are the previous one
        cur_record = item
        cur_datetime = datetime.combine(cur_record['event_date'], datetime.min.time()) + cur_record['event_time']

        if cur_record['device_id'] == ref_record['device_id']:
            if cur_record['event_date'] == ref_record['event_date']:
                td = cur_datetime - ref_datetime
                viewtime = td.seconds + td.days*24*3600
            else:
                td = (datetime.combine(ref_record['event_date'], datetime.max.time()) - ref_datetime)
                viewtime = td.seconds + td.days*24*3600
        else:
            # then viewtime is referenced from datetime.max.time()
            td = (datetime.combine(ref_record['event_date'], datetime.max.time()) - ref_datetime)
            viewtime = td.seconds + td.days*24*3600

        # Add the program name info
        try:
            program_name = tms_id_prog_name_dict[cur_record['program_tms_id']]
        except:
            program_name = None
        cur_record['program_name'] = program_name

        ref_record['viewtime'] = viewtime
        ref_record = cur_record
        ref_datetime = cur_datetime
    # For last record
    td = (datetime.combine(ref_record['event_date'], datetime.max.time()) - ref_datetime)
    viewtime = td.seconds + td.days*24*3600
    ref_record['viewtime'] = viewtime

# 68,400(7PM) - 79,200(10 PM) s
# This can be generic
def filter_7PM_10PM(records):
    # Filter records for primetime
    start = 68400.0
    end = 79200.0
    
    new_records = list()
    for item in records:
        td = item['event_time']
        event_time_secs = td.seconds + td.days*24*3600
        if event_time_secs >= start and event_time_secs < end:
            new_records.append(item)

    return new_records

# Read all the 1 (50K) devices info
def read_all_device_info(db, db_table_name, tms_id_prog_name_dict):
    
    query = """
        SELECT DISTINCT device_id FROM {0}
        LIMIT 5000;
        """.format(db_table_name)
    result = db.query(query)

    listOfDevices = list()
    for item in result:
        listOfDevices.append(item['device_id'])
    #print listOfDevices
    
    query = """
        SELECT device_id, event_date, event_time, program_tms_id FROM {0}
        WHERE device_id IN %s
        ORDER BY device_id, event_date, event_time
        ;
        """.format(db_table_name)
    result = db.query_new(query, listOfDevices)
    add_viewtime_info(result, tms_id_prog_name_dict)
    #print result
    #for item in result:
    #    print item

    filtered_result = filter_7PM_10PM(result)

    print 'TOTAL Result Count', len(result)
    print 'Filtered Result Count', len(filtered_result)
    #return result
    return filtered_result

# Get all the program listing
def get_all_program_listing(current_year, current_month):
    
    #base_dir = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data'
    #programs_dir = os.path.join(os.path.sep, base_dir, 'rpt_programs_04')
    base_dir = '/files2/Temp/fwm_' + current_month
    programs_dir = os.path.join(os.path.sep, base_dir, 'programs')
   
    programs_file_prefix = '4Cinsights.rpt_programs.date_2014-' + current_month+ '-'
    programs_file_suffix = '.pd'

    # Parameters for looping
    start_date = datetime.strptime(current_year+current_month+'01', '%Y%m%d')
    start = 1
    end = 28
    if start_date.strftime('%b') in months_with_30_days:
        end = 30
    elif start_date.strftime('%b') in months_with_31_days:
        end = 31

    tms_id_prog_name_dict = dict()
    print 'Beging Processing ...'
    for i in range(start, end+1):
        programs_filename = programs_file_prefix
        if i < 10:
            programs_filename = programs_filename + str(0) + str(i) + programs_file_suffix
        else:
            programs_filename = programs_filename + str(i) + programs_file_suffix

        programs_abs_file_path = os.path.join(os.path.sep, programs_dir, programs_filename)
        #print programs_abs_file_path

        #DRY-RUN#continue
        #pv_abs_file_path = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_pv_1M.log'
        #rawxml_abs_file_path = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_raw_1M.log'
        if os.path.isfile(programs_abs_file_path):
            #print 'Starting to analyze file: ', programs_abs_file_path, ' at: ', datetime.now()
            fp = open(programs_abs_file_path)
            for line in fp:
                entity = line.strip('\t\n\r ').split('|')
                tms_id_prog_name_dict[entity[0]] = entity[1]
            fp.close()
            #break
        else:
            print 'Error: File Doesnt exists, ', programs_abs_file_path

    print 'length of tms_id_prog_name_dict:',
    print len(tms_id_prog_name_dict)
    return tms_id_prog_name_dict

def create_device_program_matrix(device_info):
    temp_d_list = list()
    temp_p_list = list()
    for item in device_info:
        temp_d_list.append(item['device_id'])
        if item['program_name']:
            temp_p_list.append(item['program_name'])
    # Get a list of unique devices
    u_device_id = list(set(temp_d_list))
    u_program_name = list(set(temp_p_list))

    #print len(u_device_id)
    #print len(u_program_name)

    # Create a numpy array with 0.0 initialization
    device_program_matrix = np.zeros(shape=(len(u_device_id), len(u_program_name)))

    # Create a hash of device id and program
    device_id_dict = dict()
    program_name_dict = dict()
    count = 0
    for item in u_device_id:
        device_id_dict[item] = count
        count = count+1
    count = 0
    for item in u_program_name:
        program_name_dict[item] = count
        count = count+1
    #print len(device_id_dict)
    #print len(program_name_dict)

    for item in device_info:
        idx_d = device_id_dict[item['device_id']]
        if item['program_name']:
            idx_p = program_name_dict[item['program_name']]
            device_program_matrix[idx_d][idx_p] = device_program_matrix[idx_d][idx_p] + item['viewtime']

    #print device_program_matrix

    print 'Writing to a file'
    fp = open('test_primetime_10K.csv', 'w')
    write_str = 'DEVICE ID'
    for item in u_program_name:
        write_str = write_str + ',' + '"' + item.strip('"') + '"'
    fp.write(write_str+'\n')
    for i in range(0, len(u_device_id)):
        write_str = str(u_device_id[i])
        for val in device_program_matrix[i]:
            write_str = write_str + ',' + str(val)
        fp.write(write_str+'\n')
    fp.close()

# Main script, reads the input files and create list of objects
def main(scriptName, argv):

    # CHANGE THIS FOR DIFFERENT MONTHS
    current_year = '2014'
    current_month = '04'

    # Get the table name, Each month has its own table
    db_table_name = 'fwm_tv_' + current_month + '_2014_new'
    db = fwm_config.Database()

    # Create a mapping of tms_id and program name for all the 30 days 
    tms_id_prog_name_dict = get_all_program_listing(current_year, current_month)

    # Read the information from the database
    device_info = read_all_device_info(db, db_table_name, tms_id_prog_name_dict)

    # Create device-program name matrix
    create_device_program_matrix(device_info)
    
    return

if __name__ == '__main__':
    #main()
    main(sys.argv[0], sys.argv[1:])
