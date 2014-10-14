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

#R## Logging module for fwm
#R#from fwm_logger_module import FWMLogger

# Configuration containing the DB name and login credentials
config = fwm_config.Config()

#R## Logger object
#R#logger = TwitterHashtagLogger().logger
#R## examples : logger.error('This is an error')
#R## examples : logger.info('This is just an Informational message')
#R## examples : logger.warning('This is a warning message')

# Time formats required
datetimeFormat = '%Y-%m-%dT%H:%MZ'
datetimeFormat1 = '%Y-%m-%d %H:%M:%S'
idFormat = '%Y_%m_%d_%H_%M'
dateFormat = '%Y-%m-%d'
sqlTimeFormat = '%Y-%m-%d %H:%M:%S'
runtimeFormat = 'PT%HH%MM'
fileSuffixFormat = '%Y_%m_%d'

# FWM TIME FORMATS
STR2TIME_FMT = '%m/%d/%y %H%M%S'
STR2TIME_FMT2 = '%Y%m%d %H%M%S'
TIME2STR_FMT = '%m_%d_%Y'
TIME2STR_FMT2 = '%m/%d/%y'
KEY_FMT = '%Y%m%d_%H%M%S'

# TODO : Not sure if this is required
VIEWTIME_THRESHOLD = 7200.0
SHOWTIME_THRESHOLD = 3600.0

# Constants
months_with_30_days = ['Apr', 'Jun', 'Sep', 'Nov']
months_with_31_days = ['Jan', 'Mar', 'May', 'Jul', 'Aug', 'Oct', 'Dec']

BULK_INSERT_MAX_COUNT = 40000

# Convert date and time string to datetime object
def to_datetime(date_str, time_str):
    # Return datetime object from date_str and time_str
    return datetime.fromtimestamp(time.mktime(time.strptime(date_str + ' ' + time_str, STR2TIME_FMT)))

def to_datetime2(date_str, time_str):
    # Return datetime object from date_str and time_str
    return datetime.fromtimestamp(time.mktime(time.strptime(date_str + ' ' + time_str, STR2TIME_FMT2)))

# Viewtime of program1
# program2 (switch time) - program1 (switch time)
def get_viewtime(program1, program2):
    return (to_datetime(program2[2], program2[3])-to_datetime(program1[2], program1[3])).total_seconds()

def get_lastshow_viewtime(program1, threshold):
    #viewtime = (to_datetime(program2[2], program2[3])-to_datetime(program1[2], program1[3])).total_seconds()
    viewtime = 24*60*60.0 - (to_datetime(program1[2], program1[3])-to_datetime(program1[2], '000000')).total_seconds()

    if viewtime > SHOWTIME_THRESHOLD:
        viewtime = SHOWTIME_THRESHOLD

    return viewtime

# Create date string to date object hash-map
def create_date_dict(start_year_month):
    date_dict = dict()
    
    start_date_str = start_year_month + '01'
    date_str_prefix = start_year_month

    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    start = 1
    end = 28
    if start_date.strftime('%b') in months_with_30_days:
        end = 30
    elif start_date.strftime('%b') in months_with_31_days:
        end = 31

    for i in range(start, end+1):
        if i < 10:
            date_str = date_str_prefix + str(0) + str(i)
        else:
            date_str = date_str_prefix + str(i)

        # Add to dictionary
        date_dict[date_str] = start_date.date()
        # Increment to the next day
        start_date = start_date + timedelta(hours=24)

    #DEBUG#print len(date_dict)
    #DEBUG##print date_dict
    #DEBUG#for item in date_dict.keys():
    #DEBUG#    print item, date_dict[item]
    return date_dict

# Create date string to day hash-map
def create_day_dict(start_year_month):
    day_dict = dict()
    
    start_date_str = start_year_month + '01'
    date_str_prefix = start_year_month

    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    start = 1
    end = 28
    if start_date.strftime('%b') in months_with_30_days:
        end = 30
    elif start_date.strftime('%b') in months_with_31_days:
        end = 31

    for i in range(start, end+1):
        if i < 10:
            date_str = date_str_prefix + str(0) + str(i)
        else:
            date_str = date_str_prefix + str(i)

        # Add to the dictionary
        day_dict[date_str] = start_date.strftime('%A')
        # Increment to the next day
        start_date = start_date + timedelta(hours=24)

    #DEBUG#print len(day_dict)
    #DEBUG##print day_dict
    #DEBUG#for item in day_dict.keys():
    #DEBUG#    print item, day_dict[item]
    return day_dict

# Create time string to time object hash-map
def create_time_dict():
    time_dict = dict()

    # This start date is redundant here, just want to start with any datetime object
    start_date_str = '20140601'
    start_time_str = '000000'

    start_datetime = datetime.strptime(start_date_str+ ' ' + start_time_str, '%Y%m%d %H%M%S')
    for h in range(0,24):
        h_str = None
        if h < 10:
            h_str = str(0) + str(h)
        else:
            h_str = str(h)
        for m in range(0,60):
            m_str = None
            if m < 10:
                m_str = str(0) + str(m)
            else:
                m_str = str(m)
            for s in range(0,60):
                s_str = None
                if s < 10:
                    s_str = str(0) + str(s)
                else:
                    s_str = str(s)

                time_str = h_str+m_str+s_str
                time_dict[int(time_str)] = start_datetime.time()
                start_datetime = start_datetime + timedelta(seconds=1)

    #DEBUG##print time_dict
    #DEBUG#print len(time_dict)
    #DEBUG#for item in time_dict.keys():
    #DEBUG#    print item, time_dict[item]
    return time_dict

# Do a bulk insert
def mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_data):
    
    # Insert query
    query = """
        INSERT INTO {0}
        (mso, device_id, event_date, event_day, event_time, event_type, event_value, event_name, event_id)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """.format(db_table_name)
   
    #print query
    #print mysql_data
    db.bulk_insert_no_commit(query, mysql_data)

def read_rawxml(rawxml_filename, db_table_name, date_dict, time_dict, day_dict):
    print 'Reading the raw xml file'
   
    db = fwm_config.Database()

    # Open the file for reading
    fp = open(rawxml_filename)

    mysql_bulk_data = list()

    for line in fp:
        
        entity_dict = dict()
        entity = line.strip('\n\t\r ').split('|')

        # Create a list of input data
        mysql_data = list()
        mysql_data.append(int(entity[0]))
        mysql_data.append(entity[1])
        mysql_data.append(date_dict[entity[2]])
        mysql_data.append(day_dict[entity[2]])
        mysql_data.append(time_dict[int(entity[3])])
        mysql_data.append(entity[4])
        mysql_data.append(int(entity[5]) if entity[5] != 'NULL' else None)
        mysql_data.append(entity[6])
        mysql_data.append(int(entity[7]) if len(entity[7]) else None)
        # Create a bulk data request
        mysql_bulk_data.append(mysql_data)

        # Debug
        if len(mysql_bulk_data) == BULK_INSERT_MAX_COUNT:
        ##if len(mysql_bulk_data) == 4:
            print 'Inserting into database ', len(mysql_bulk_data), ' rows'

            # Call bulk insert
            mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_bulk_data)
            # Reset this
            mysql_bulk_data = list()

            db.explicit_commit()
            
            ### DEBUG
            ##break

    if len(mysql_bulk_data):
        print 'Inserting into database ', len(mysql_bulk_data), ' rows'
        mysqldb_bulk_insert_into_fwm_tv(db, mysql_bulk_data)

    # Do one more final commit
    db.explicit_commit()
    
    # Close the file once done
    fp.close()
   
def read_program_view_from_file(program_view_filename):
    print 'Reading program view from file'

    # New Code using pandas
    # Since each file is around 1.5GB, you can read the entire file in a go
    # TODO Maybe later if the file size increase we might not be able to do this
    file_contents = pd.read_csv(program_view_filename, sep='|', header=None)

    #######
    # use groupby function to result a sorted list based on dev_id
    # device_idx_list=list()
    # dev_idx_dict = file_contents.groupby(file_contents[1]).groups
    # for dev_id in dev_idx_dict.keys():
    #    dev_idx = dev_idx_dict[dev_id]
    #    device_idx_list.append([dev_id,dev_idx[0],dev_idx[-1]+1])
    # return (file_contents, device_idx_list)
    #######

    contents_shape = file_contents.shape
    #print contents_shape
    
    # Get a unique list of devices
    device_array = np.unique(file_contents[1])

    # Takes just 31.015s
    # Create the device id and the start and end indices
    idx = 0
    cur_dev = None
    last_dev = None
    cur_idx = 0
    last_idx = -1
    device_idx_list = list()
    flag = True
    for item in file_contents[1]:
        if cur_dev != str(item):
            last_dev = cur_dev
            cur_dev = str(item)
            last_idx = cur_idx
            cur_idx = idx
            if flag:
                flag = False
            else:
                device_idx_list.append([last_dev, last_idx, idx])

        idx = idx + 1
    # Add the last index
    device_idx_list.append([cur_dev, cur_idx, contents_shape[0]])

    #DEBUG## This takes 1m18s (including the above)
    #DEBUG#file_contents_np = np.array(file_contents)
    #DEBUG#dummy = None
    #DEBUG#for item in device_idx_list:
    #DEBUG#    #print start, end
    #DEBUG#    dummy = file_contents_np[item[1]:item[2], :]

    return (file_contents, device_idx_list)

def read_rawxml_from_file(rawxml_filename):
    print 'Reading rawxml from file'

    # New Code using pandas
    # Since each file is around 1.5GB, you can read the entire file in a go
    # TODO Maybe later if the file size increase we might not be able to do this
    file_contents = pd.read_csv(rawxml_filename, sep='|', header=None)
    contents_shape = file_contents.shape

    device_array = np.unique(file_contents[1])

    # Create the device id and the start and end indices
    idx = 0
    cur_dev = None
    last_dev = None
    cur_idx = 0
    last_idx = -1
    device_idx_list = list()
    device_idx_dict = dict()
    flag = True
    for item in file_contents[1]:
        if cur_dev != str(item):
            last_dev = cur_dev
            cur_dev = str(item)
            last_idx = cur_idx
            cur_idx = idx
            if flag:
                flag = False
            else:
                #device_idx_list.append([last_dev, last_idx, idx])
                device_idx_dict[last_dev] = [last_idx, idx]

        idx = idx + 1
    # Add the last index
    #device_idx_list.append([cur_dev, cur_idx, contents_shape[0]])
    device_idx_dict[cur_dev] = [cur_idx, contents_shape[0]]
    print len(device_idx_dict)
    #return (np.array(file_contents), device_idx_dict)
    return (file_contents, device_idx_dict)

# Do a bulk insert
def mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_data):
    
    # Insert query
    query = """
        INSERT INTO {0}
        (mso, device_id, event_date, event_day, event_time, event_type, event_value, event_name, event_id, program_tms_id)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """.format(db_table_name)
   
    #print query
    #print mysql_data
    db.bulk_insert_no_commit(query, mysql_data)

# Merge the two slices (raw, and program_view)
def merge_data(raw_slice, pv_slice, date_dict, time_dict, day_dict):

    raw_slice = np.array(raw_slice)
    pv_slice = np.array(pv_slice)
    #raw_slice = np.array(raw_slice).tolist()
    #pv_slice = np.array(pv_slice).tolist()
    raw_idx = 0
    pv_idx = 0

    tv_info = list()
    while True:

        if raw_idx >= len(raw_slice) or pv_idx >= len(pv_slice):
            break

        cur_raw_data = raw_slice[raw_idx]
        cur_pv_data = pv_slice[pv_idx]

        # Compare if the times are equal from raw data and program view data
        if int(cur_raw_data[3]) == int(cur_pv_data[3]):
            raw_idx = raw_idx+1
            pv_idx = pv_idx+1

            # ex. mso, device_id, event_date, event_day, event_time, event_type, event_value, event_name, event_id, program_tms_id
            if cur_raw_data[0] == cur_pv_data[0] and cur_raw_data[7] == float(cur_pv_data[4]):
                cur_raw_data = cur_raw_data.tolist()
                event_value = int(cur_raw_data[5]) if not np.isnan(cur_raw_data[5]) else None
                data = cur_raw_data[0:2] + [date_dict[str(cur_raw_data[2])], day_dict[str(cur_raw_data[2])], time_dict[cur_raw_data[3]], cur_raw_data[4], event_value, cur_raw_data[6], int(cur_raw_data[7]), cur_pv_data[5]]
                tv_info.append(data)
            else:
                # Raw Data
                cur_raw_data = cur_raw_data.tolist()
                event_value = int(cur_raw_data[5]) if not np.isnan(cur_raw_data[5]) else None
                event_id = int(cur_raw_data[7]) if not np.isnan(cur_raw_data[7]) else None
                data = cur_raw_data[0:2] + [date_dict[str(cur_raw_data[2])], day_dict[str(cur_raw_data[2])], time_dict[cur_raw_data[3]], cur_raw_data[4], event_value, cur_raw_data[6], event_id, None]
                tv_info.append(data)
                
                # Program View Data
                cur_pv_data = cur_pv_data.tolist()
                data = cur_pv_data[0:2] + [date_dict[str(cur_pv_data[2])], day_dict[str(cur_pv_data[2])], time_dict[cur_pv_data[3]]] + [None, None, None] + cur_pv_data[4:]
                tv_info.append(data)

        elif int(cur_raw_data[3]) < int(cur_pv_data[3]):
            raw_idx = raw_idx+1
            cur_raw_data = cur_raw_data.tolist()
            event_value = int(cur_raw_data[5]) if not np.isnan(cur_raw_data[5]) else None
            event_id = int(cur_raw_data[7]) if not np.isnan(cur_raw_data[7]) else None
            data = cur_raw_data[0:2] + [date_dict[str(cur_raw_data[2])], day_dict[str(cur_raw_data[2])], time_dict[cur_raw_data[3]], cur_raw_data[4], event_value, cur_raw_data[6], event_id, None]
            tv_info.append(data)
        else:
            # Populate more information here
            pv_idx = pv_idx+1
            cur_pv_data = cur_pv_data.tolist()
            data = cur_pv_data[0:2] + [date_dict[str(cur_pv_data[2])], day_dict[str(cur_pv_data[2])], time_dict[cur_pv_data[3]]] + [None, None, None] + cur_pv_data[4:]
            tv_info.append(data)

    # Insert items from prog veiw file in case they are skipped
    # TODO I am not adding extra programs from rawxml file as there wont be any program information
    # Adding more programs to the file
    while pv_idx < len(pv_slice):
        cur_pv_data = pv_slice[pv_idx]
        cur_pv_data = cur_pv_data.tolist()
        data = cur_pv_data[0:2] + [date_dict[str(cur_pv_data[2])], day_dict[str(cur_pv_data[2])], time_dict[cur_pv_data[3]]] + [None, None, None] + cur_pv_data[4:]
        tv_info.append(data)
        # Increment the index
        pv_idx = pv_idx+1
   
    # Insert items from rawxml
    while raw_idx < len(raw_slice):
        cur_raw_data = raw_slice[raw_idx]
        cur_raw_data = cur_raw_data.tolist()
        event_value = int(cur_raw_data[5]) if not np.isnan(cur_raw_data[5]) else None
        event_id = int(cur_raw_data[7]) if not np.isnan(cur_raw_data[7]) else None
        data = cur_raw_data[0:2] + [date_dict[str(cur_raw_data[2])], day_dict[str(cur_raw_data[2])], time_dict[cur_raw_data[3]], cur_raw_data[4], event_value, cur_raw_data[6], event_id, None]
        tv_info.append(data)
        # Increment the index
        raw_idx = raw_idx+1

    #for item in tv_info:
    #    print item
    # You could possibly insert into the database here
    return tv_info

def process_files(rawxml_filename, pv_filename, db, db_table_name, date_dict, day_dict, time_dict):
    rawxml_filename = rawxml_filename
    program_view_filename = pv_filename
   
    # Read and process the files
    pv_file_contents, pv_device_idx_list = read_program_view_from_file(program_view_filename)
    raw_file_contents, raw_device_idx_dict = read_rawxml_from_file(rawxml_filename)
    
    print 'RETURNED FILE READ'
    #aa = np.array(raw_file_contents)
    #aa = raw_file_contents.values
    # This will give you rows from 4, to 10 (last one excluding)
    #aa = raw_file_contents[4:10]
    #print np.array(aa)

    # Iterate over all the devices in the program_view file
    mysql_bulk_data = list()
    count = 0
    total_count = 0
    tt = 0
    for item in pv_device_idx_list:
        pv_slice = pv_file_contents[item[1]:item[2]]
     
        try:
            raw_start_idx = raw_device_idx_dict[item[0]][0]
            raw_end_idx = raw_device_idx_dict[item[0]][1]
            raw_slice = raw_file_contents[raw_start_idx: raw_end_idx]

            # Do the merging
            count = count+1
            mysql_data = merge_data(raw_slice, pv_slice, date_dict, time_dict, day_dict)
            mysql_bulk_data.extend(mysql_data)

            if len(mysql_bulk_data) > 10000:
                print 'Length of data: ', len(mysql_bulk_data)
                total_count = total_count + len(mysql_bulk_data)
                mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_bulk_data[:9000])
                db.explicit_commit()
                mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_bulk_data[9000:])
                db.explicit_commit()
    
                print 'Split', len(mysql_bulk_data[:9000]), len(mysql_bulk_data[9000:])
                # Reset the counter
                count = 0
                mysql_bulk_data = list()

        except KeyError:
            continue

        # Testing
        #break
    if len(mysql_bulk_data):
        total_count = total_count + len(mysql_bulk_data)
        if len(mysql_bulk_data) < 9500:
            # do something
            mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_bulk_data)
            db.explicit_commit()
            mysql_bulk_data = list()
        else:
            # split
            mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_bulk_data[:9000])
            db.explicit_commit()
            mysqldb_bulk_insert_into_fwm_tv(db, db_table_name, mysql_bulk_data[9000:])
            db.explicit_commit()
            mysql_bulk_data = list()

    print 'TOTAL COUNT: ', total_count

# Main script, reads the input files and create list of objects
def main(scriptName, argv):
    
    # CHANGE THIS FOR DIFFERENT MONTHS
    current_year = '2014'
    current_month = '04'

    # Create a dictionary of dates
    date_dict = create_date_dict(current_year + current_month)
    time_dict = create_time_dict()
    day_dict = create_day_dict(current_year + current_month)
    
    # Get the table name, Each month has its own table
    db_table_name = 'fwm_tv_' + current_month + '_2014_test'
    db = fwm_config.Database()

    # Testing
    #program_view_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/4Cinsights.rpt_prog_view.date_2014-04-02.2014-06-09.pd'
    #program_view_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_pv_100000.pd'
    #program_view_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_pv_10000.log'
    #program_view_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_pv_1M.log'
    program_view_filename = '/Users/fan/Desktop/intern/real_data/4Cinsights.rpt_prog_view.date_2014-04-01.2014-06-09.pd'
    #program_view_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_pv_100000.log'
    #rawxml_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/FWM_20140402_R.pd'
    #rawxml_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_5000.log'
    #rawxml_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_raw_10000.log'
    #rawxml_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_raw_1M.log'
    rawxml_filename = '/Users/fan/Desktop/intern/real_data/FWM_20140401_R.pd'
    #rawxml_filename = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_raw_100000.log'
    print 'Starting to analyze file: ', rawxml_filename, program_view_filename, ' at: ', datetime.now()
    process_files(rawxml_filename, program_view_filename, db, db_table_name, date_dict, day_dict, time_dict)
    print 'Done working with file: ', rawxml_filename, program_view_filename, ' at: ', datetime.now()
    return
    
    # DEBUG
    #read_rawxml('/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_1000000.log', db_table_name, date_dict, time_dict, day_dict)
    #FULL#read_rawxml('/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/FWM_20140625_N.pd', date_dict, time_dict, day_dict)

    # Files prefix and suffix
    #base_dir = '/files2/Temp/fwm_' + current_month
    #raw_xml_dir = os.path.join(os.path.sep, base_dir, 'rawxml')
    #prog_view_dir = os.path.join(os.path.sep, base_dir, 'prog_view')
    base_dir = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data'
    raw_xml_dir = os.path.join(os.path.sep, base_dir, '')
    prog_view_dir = os.path.join(os.path.sep, base_dir, '')
    
    rawxml_file_prefix = 'FWM_' + current_year + current_month
    rawxml_file_suffix = '_R.pd'

    pv_file_prefix = '4Cinsights.rpt_prog_view.date_' + current_year + '-' + current_month
    pv_file_suffix = '.2014-06-09.pd'

    # Parameters for looping
    start_date = datetime.strptime(current_year+current_month+'01', '%Y%m%d')
    start = 1
    end = 28
    if start_date.strftime('%b') in months_with_30_days:
        end = 30
    elif start_date.strftime('%b') in months_with_31_days:
        end = 31

    print 'Beging Processing ...'
    for i in range(start, end+1):
        rawxml_filename = rawxml_file_prefix
        pv_filename = pv_file_prefix
        if i < 10:
            rawxml_filename = rawxml_filename + str(0) + str(i) + rawxml_file_suffix
            pv_filename = pv_filename + '-' + str(0) + str(i) + pv_file_suffix
        else:
            rawxml_filename = rawxml_filename + str(i) + rawxml_file_suffix
            pv_filename = pv_filename + '-' + str(i) + pv_file_suffix

        rawxml_abs_file_path = os.path.join(os.path.sep, raw_xml_dir, rawxml_filename)
        pv_abs_file_path = os.path.join(os.path.sep, prog_view_dir, pv_filename)
        #print rawxml_abs_file_path, pv_abs_file_path
        print rawxml_abs_file_path
        print pv_abs_file_path

        pv_abs_file_path = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_pv_1M.log'
        rawxml_abs_file_path = '/Volumes/Work/VoxsupInc/FourthWallMedia/data/new_data/out_raw_1M.log'
        if os.path.isfile(rawxml_abs_file_path) and os.path.isfile(pv_abs_file_path):
            print 'Starting to analyze file: ', rawxml_abs_file_path, pv_abs_file_path, ' at: ', datetime.now()
            process_files(rawxml_abs_file_path, pv_abs_file_path, db, db_table_name, date_dict, day_dict, time_dict)
            break
        else:
            print 'Error: Either or both File Doesnt exists, ', rawxml_abs_file_path, pv_abs_file_path

        print 'Done working with file: ', rawxml_abs_file_path, pv_abs_file_path, ' at: ', datetime.now()
        print ''

        #break

    print 'ALL DONE ...'
    return

if __name__ == '__main__':
    #main()
    main(sys.argv[0], sys.argv[1:])
