import sys
import pandas as pd
import numpy as np
from datetime import date,time,datetime,timedelta
import calendar
from loyalty_analysis import viewtime_threshold
import fwm_config as fwm_config

def get_daily_prog_viewtime(db,start_date,end_date,dma,prime_start):
    if start_date.month<10:
        month = '0%s'%start_date.month
    else:
        month = start_date.month
    db_tv_table = 'fwm_tv_%s_2014'%month
    db_ref_table = 'fwm_ref_%s_2014'%month
    query = """
            SELECT DISTINCT device_id FROM {0}
            WHERE dma_code = %s
            """.format(db_ref_table)
    result = db.query_new(query,dma)
    dev_list = map(lambda x:x['device_id'],result)
    prime_start_time = time(prime_start)
    today = datetime.today()
    tmp = datetime(today.year,today.month,today.day,prime_start)
    prime_end_time = (tmp+timedelta(hours=2,minutes=59,seconds=59)).time()
    query_data = """
                SELECT device_id,event_day,event_time,event_id,program_title,program_viewtime FROM {0}
                WHERE device_id IN %s AND event_date BETWEEN %s AND %s
                AND event_id IS NOT NULL AND program_title IS NOT NULL
                AND event_time BETWEEN %s AND %s;
                """.format(db_tv_table)
    results = db.query_tuple(query_data, (dev_list,start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'),str(prime_start_time),str(prime_end_time)))
    return list(results)

def get_network_info(event_id,*args):
    network_dict = args[0]
    try:
        network_name_list = network_dict[event_id]
        if len(network_name_list)>1:
            print 'multiple network names for event_id.',event_id,network_name_list
        return network_name_list[0]
    except KeyError:
        print 'No network name  found for event id:',event_id
        return 'Unknown'

def get_network_dict(db):
    db_network_table = 'networks'
    query = """
            SELECT station_id, IF(affiliate_call_sign IS NULL, call_sign, affiliate_call_sign) AS network_name
            FROM {0};
            """.format(db_network_table)
    results = db.query(query)
    network_dict = dict()
    for entry in results:
        network_dict.setdefault(entry['station_id'],[]).append(entry['network_name'])
    return network_dict
    #print network_dict[10035]

def get_prog_timeslots(db,dma,start_date,end_date,prime_start,network_dict):
    if start_date.month < 10:
        month = '0%s'%start_date.month
    else:
        month = start_date.month
    db_tv_table = 'fwm_tv_%s_2014'%month
    db_ref_table = 'fwm_ref_%s_2014' %month
    query = """
            SELECT DISTINCT device_id FROM {0}
            WHERE dma_code = %s
            """.format(db_ref_table)
    result = db.query_new(query, dma)
    dev_list = map(lambda x : x['device_id'], result)
    #print 'number of devices got from ref_table:',len(dev_list)
    prime_start_time = time(prime_start)
    today = datetime.today()
    tmp = datetime(today.year,today.month,today.day,prime_start)
    prime_end_time = (tmp + timedelta(hours=2,minutes=59,seconds=59)).time()
    #print 'prime time period:',str(prime_start_time),str(prime_end_time)
    query_data = """
                SELECT event_day, event_time, event_id, program_title FROM {0}
                WHERE device_id IN %s AND event_date BETWEEN %s AND %s
                AND event_id IS NOT NULL AND program_title IS NOT NULL
                AND event_time BETWEEN %s AND %s;
                """.format(db_tv_table)
    #print 'dev_list length:',len(dev_list)
    print 'start_date and end_date:',start_date,end_date
    result = db.query_tuple(query_data, (dev_list,start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'),str(prime_start_time),str(prime_end_time)))
    #print 'result count ', len(result)
    prog_timeslot_dict = dict()
    for entry in result:
        prog_title = entry['program_title'].replace(',',' ')
        if entry['program_title'] == '2014 NBA Finals':
            prog_title = 'NBA Basketball'
        try:
            network_name_list = network_dict[entry['event_id']]
            if len(network_name_list)>1:
                print 'multiple network names for event_id.',entry['event_id'],network_name_list
            network_name = network_name_list[0]
        except KeyError:
            print 'No network name  found for event id:',entry['event_id']
            network_name = 'Unknown'
        prog_title += '_' + network_name + '_' + entry['event_day']
        prog_timeslot_dict.setdefault(prog_title,set()).add(str(entry['event_time'].seconds/1800))
    #print result[0]['event_time'].seconds / (30*60)
    #print prog_timeslot_dict.items()[0]
    return prog_timeslot_dict

def get_first_timeslot(prog_title, *args):
    prog_timeslot_dict = args[0]
    try:
        timeslots = prog_timeslot_dict[prog_title]
        return min(list(timeslots))
    except KeyError:
        print 'no timeslots found for program:',prog_title
        return -1

def get_timeslot_num(prog_title,prog_timeslot_dict):
    try:
        timeslots = prog_timeslot_dict[prog_title]
        return len(timeslots)
    except KeyError:
        print 'no timeslots found for program:',prog_title
        return 0

def create_daily_viewtime_matrix(dma, start_date, end_date, prime_start):
    db = fwm_config.Database()
    if end_date.month != start_date.month:
        lastday = calendar.monthrange(2014,start_date.month)[1]
        end_date_tmp = date(2014,start_date.month,lastday)
        start_date_tmp = date(2014,end_date.month,1)
        viewtime_data = get_daily_prog_viewtime(db,start_date,end_date,dma,prime_start)
        viewtime_data.extend(get_daily_prog_viewtime(db,start_date,end_date,dma,prime_start))
    else:
        viewtime_data = get_daily_prog_viewtime(db,start_date,end_date,dma,prime_start)

    data_df = pd.DataFrame(viewtime_data)
    network_dict = get_network_dict(db)
    prog_timeslot_dict = get_prog_timeslots(db,dma,start_date,end_date,prime_start,network_dict)
    data_df.loc[data_df.program_title=='2014 NBA Finals','program_title']='NBA Basketball'
    data_df['program_title'] = data_df['program_title'].map(lambda x:x.replace(',',' '))
    data_df['program_viewtime'] = data_df['program_viewtime'].map(viewtime_threshold)
    #data_df['program_network'] = data_df['event_id'].map(get_network_info)
    data_df['program_network'] = data_df['event_id'].apply(get_network_info,args=(network_dict,))
    data_df['program_title'] = data_df['program_title']+'_'+data_df['program_network']+'_'+data_df['event_day']
    data_df['first_timeslot'] = data_df['program_title'].apply(get_first_timeslot,args=(prog_timeslot_dict,))
    data_df['program_title_time'] = data_df['program_title']+'_'+data_df['first_timeslot']
    #print data_df.head()

    viewtime_matrix = dict()
    for prog_dev, group in data_df.groupby(['program_title_time','device_id','program_title']):
        viewtime = group['program_viewtime'].sum()
        viewtime_matrix.setdefault(prog_dev[0],{})[prog_dev[1]] = viewtime*1.0/get_timeslot_num(prog_dev[2],prog_timeslot_dict)
    re_df = pd.DataFrame(viewtime_matrix)
    re_df = re_df.fillna(0)
    re_df = re_df[re_df.sum(axis=1) > 15*60*7]
    re_df = re_df[(re_df.T != 0).any()]
    re_df = re_df[re_df.columns[(re_df!=0).any()]]
    re_df.rename(columns=lambda x:x.replace(',',' '),inplace=True)
    #print re_df.head()
    #return
    re_df.to_csv('output_viewtime_daily_matrix_%s_%s_%s.csv'%(start_date.month,start_date.day,dma), sep=',')

if __name__=='__main__':
    #create_daily_viewtime_matrix(618,date(2014,4,1),date(2014,4,1),19)
    #sys.exit()
    start_date = date(2014,4,1)
    end_date = date(2014,6,29)

    iter_date = start_date
    while iter_date <= end_date:
        print 'Start analyzing week:',iter_date,' at ',datetime.now()
        week_end = iter_date + timedelta(days=6-iter_date.weekday())
        if week_end > end_date:
            #create_daily_viewtime_matrix(501,iter_date,end_date,20)
            create_daily_viewtime_matrix(618,iter_date,end_date,19)
        else:
            #create_daily_viewtime_matrix(501,iter_date,week_end,20)
            create_daily_viewtime_matrix(618,iter_date,week_end,19)
        iter_date += timedelta(days = 7-iter_date.weekday())
    print 'All done at ',datetime.now()
