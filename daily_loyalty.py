import MySQLdb
import pandas as pd
import numpy as np
from datetime import date,datetime,time,timedelta
import calendar

import fwm_config as fwm_config
#from loyalty_analysis import viewtime_threshold

def get_prog_viewtime_matrix(db,start_date,dma_code,mso):
    start_date0 = start_date
    delta = timedelta(days=6)
    end_date1 = start_date + delta
    if end_date1.month != start_date.month:
        days_number = calendar.monthrange(2014,start_date0.month)[1]
        end_date0 = date(2014,start_date0.month,days_number)
        start_date1 = date(2014,end_date1.month,1)
        matrix = get_prog_viewtime_matrix_within_month(db,start_date0,end_date0,dma_code,mso)
        matrix.extend(get_prog_viewtime_matrix_within_month(db,start_date1,end_date1,dma_code,mso))
    else:
        matrix = get_prog_viewtime_matrix_within_month(db,start_date0,end_date1,dma_code,mso)
    return matrix

def get_prog_viewtime_matrix_within_month(db,start_date,end_date,dma_code,mso,prime_start=20):
    if start_date.month<4:
        return []
    if start_date.month<10:
        month = '0%s' %start_date.month
    else:
        month = start_date.month
    db_tv_table = 'fwm_tv_%s_2014' %month
    db_ref_table = 'fwm_ref_%s_2014' %month
    if dma_code!=-1:
        query = """
                SELECT DISTINCT device_id FROM {0}
                WHERE dma_code = %s
                """.format(db_ref_table)
        result = db.query_new(query, dma_code)
    elif mso!=-1:
        query = """
                SELECT DISTINCT device_id FROM {0}
                WHERE mso = %s
                """.format(db_ref_table)
        result = db.query_new(query, mso)
    dev_list = map(lambda x : x['device_id'], result)
    print 'number of devices got from ref_table:',len(dev_list)
    prime_start_time = time(prime_start)
    today = datetime.today()
    tmp = datetime(today.year,today.month,today.day,prime_start)
    prime_end_time = (tmp + timedelta(hours=2,minutes=59,seconds=59)).time()
    print 'prime time period:',str(prime_start_time),str(prime_end_time)
    query_data = """
                SELECT device_id, event_date, event_time, program_tms_id, program_title, program_viewtime FROM {0}
                WHERE device_id IN %s AND event_date BETWEEN %s AND %s
                AND event_id IS NOT NULL AND program_title IS NOT NULL
                AND event_time BETWEEN %s AND %s;
                """.format(db_tv_table)
    print 'dev_list length:',len(dev_list)
    print 'start_date and end_date:',start_date,end_date
    result = db.query_tuple(query_data, (dev_list,start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'),str(prime_start_time),str(prime_end_time)))
    print 'result count ', len(result)
    return list(result)

def count_dev_prog_daily(df):
    effective_dev_prog_dict = {}
    dev_prog_grouped = df.groupby(['device_id','program_title'])
    for name, group in dev_prog_grouped:
        viewtime = group['program_viewtime'].sum()
        if viewtime > 0:
            dev = name[0]
            prog = name[1]
            effective_dev_prog_dict.setdefault(prog,[]).append(dev)
    return effective_dev_prog_dict

def analyze_loyalty(prog_weekly_dev_dict_list, dma_code, mso, start_date):
    print 'Begin analyzing loyalty ...'
    prog_dict = dict()
    first_dict = prog_weekly_dev_dict_list[0]
    for prog in first_dict:
        prog_dict.setdefault(prog,{})['t0'] = len(first_dict[prog])
    
    col_name = ['t0']
    sort_col_name = ['t0'] # sort the output data based on these columns
    for i in range(1,len(prog_weekly_dev_dict_list)):
        col_name.append('t%s' %str(i))
        col_name.append('t%s-%s' %(str(i-1),str(i)))
        col_name.append('t%s-%s loyalty ratio' %(str(i-1),str(i)))
        sort_col_name.append('t%s' %str(i))
        pre_dict = prog_weekly_dev_dict_list[i-1]
        curr_dict = prog_weekly_dev_dict_list[i]
        for prog, dev_list in curr_dict.items():
            prog_dict.setdefault(prog,{})['t%s'%str(i)] = len(dev_list)
            try:
                common_np = np.intersect1d(pre_dict[prog],dev_list)
                prog_dict[prog]['t%s-%s' %(str(i-1),str(i))] = common_np.size
                if common_np.size == 0:
                    prog_dict[prog]['t%s-%s loyalty ratio' %(str(i-1),str(i))] = 0
                else:
                    prog_dict[prog]['t%s-%s loyalty ratio' %(str(i-1),str(i))] = common_np.size*1.0/len(pre_dict[prog])
            except KeyError:
                prog_dict[prog]['t%s-%s' %(str(i-1),str(i))] = 0
                prog_dict[prog]['t%s-%s loyalty ratio' %(str(i-1),str(i))] = 0
    
    prog_list = list()
    for key,value in prog_dict.items():
        value['program_title'] = key
        for i in range(0,7): # fill in missing values
            if 't%s'%i not in value:
                value['t%s'%i] = 0
        prog_list.append(value)

    df = pd.DataFrame(prog_list)
    df = df.fillna(0)
    df['viewer_mean'] = df[sort_col_name].mean(axis=1)
    df['week_number'] = start_date.isocalendar()[1]
    #df = df.sort('viewer_mean',ascending=0)
    return df

def count_dev_per_prog_weekly(dma_code,mso,start_date,end_date):
    db = fwm_config.Database()
    combined_df = pd.DataFrame()
    iter_date = start_date
    while iter_date + timedelta(days=6) <= end_date:
        print 'New week start from:',iter_date
        prog_viewtime_matrix = get_prog_viewtime_matrix(db,iter_date,dma_code,mso)
        df = pd.DataFrame(prog_viewtime_matrix)
        #df = df[df.program_tms_id is not None]
        df.loc[df.program_title=='2014 NBA Finals','program_title'] = 'NBA Basketball'
        #df['program_viewtime'] = df['program_viewtime'].map(viewtime_threshold)
        df['day'] = df['event_date'].map(lambda x : x.isoweekday())
        prog_daily_dev_dict_list = [{}]*7
        for day, daily_data in df.groupby('day'):
            print 'data of day added:',day
            prog_daily_dev_dict_list[day-1] = count_dev_prog_daily(daily_data)
        #prog_daily_dev_dict_list = map(lambda x : count_dev_prog_daily(x[1]), df.groupby('day'))
        combined_df = combined_df.append( analyze_loyalty(prog_daily_dev_dict_list, dma_code, mso, iter_date) )
        iter_date += timedelta(days=7)

    newdf = pd.DataFrame()
    for name, group in combined_df.groupby(['program_title']):
        group['viewer_total'] = group['viewer_mean'].sum()
        newdf = newdf.append(group)
    newdf = newdf.sort(['viewer_total','program_title','week_number'], ascending=[0,0,1])
    #print newdf.head()
    cols = newdf.columns.values.tolist()
    cols.remove('viewer_mean')
    cols.remove('program_title')
    cols.remove('week_number')
    cols.remove('viewer_total')
    cols.sort(key=lambda x:len(x))
    #print cols
    newdf.to_csv('output_daily_dma=%s_mso=%s.csv'%(dma_code,mso), columns = ['program_title','week_number']+cols)

if __name__=='__main__':
    start_date = date(2014,3,31)
    end_date = date(2014,6,30)
    #tasks = [(524,-1),(501,-1),(-1,6760),(-1,6330)] # (dma_code, mso)
    tasks = [(501,-1),(-1,6760)] # Atlanta
    for task in tasks:
        print 'Start to analyze task ', task, ' at: ', datetime.now()
        count_dev_per_prog_weekly(task[0], task[1], start_date, end_date)
    print 'All Done at: ', datetime.now()
