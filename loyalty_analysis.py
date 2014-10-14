import MySQLdb
import pandas as pd
import numpy as np
from datetime import datetime,date

import fwm_config as fwm_config

#from create_deviceid_program_matrix_primetime import get_all_program_listing
#from create_deviceid_program_matrix_primetime import read_all_device_info
#from create_deviceid_program_matrix_primetime import add_viewtime_info
#from create_deviceid_program_matrix_primetime import filter_7PM_10PM

def get_prog_viewtime_matrix(db, current_month, dma_code, mso):
    db_tv_table = 'fwm_tv_%s_2014' %current_month
    db_ref_table = 'fwm_ref_%s_2014' %current_month
    if dma_code!=-1:
        query_dev = """
                    SELECT DISTINCT device_id FROM {0}
                    WHERE dma_code = %s;
                    """.format(db_ref_table)
        result = db.query_new(query_dev, dma_code)
    elif mso!=-1:
        query_dev = """
                    SELECT DISTINCT device_id FROM {0}
                    WHERE mso = %s;
                    """.format(db_ref_table)
        result = db.query_new(query_dev, mso)
    dev_list = map(lambda x : x['device_id'], result)
    print 'number of devices got from ref_table:',len(dev_list)

    query_data = """
                SELECT device_id, event_date, event_time, program_tms_id, program_title, program_viewtime FROM {0}
                WHERE device_id IN %s AND event_id IS NOT NULL
                AND event_time BETWEEN '20:00:00' AND '23:00:00';
                """.format(db_tv_table)
    result = db.query_new(query_data, dev_list) # a tuple
    #add_viewtime_info(result,tms_id_prog_name_dict)
    #filtered_result = filter_7PM_10PM(result)
    print 'Result Count', len(result)
    #print 'Filtered Result Count', len(filtered_result)
    return list(result)

def viewtime_threshold(viewtime):
    return (viewtime>=300) and viewtime or 0 # 5min

#def week_number(date):
        #if(date < datetime.date(2014,4,8)):
        #	return 1

def count_dev_per_prog_weekly(df):
    #dev_prog_list = df.groupby(['device_id','program_name','week'],sort = True).sum()
    #print df.groupby(['device_id','program_name','week'],sort = True).head()
    #effective_list = dev_prog_list[dev_prog_list.viewtime > 600]
    #print dev_prog_list.columns
    #print dev_prog_list.dtypes

    effective_dev_prog_dict = dict()
    dev_prog_grouped = df.groupby(['device_id','program_title','week'])
    for name,group in dev_prog_grouped:
        #viewtime = group.sum(0)['program_viewtime']
        viewtime = group['program_viewtime'].sum()
        if viewtime > 600: # 10 min
            dev = name[0]
            prog = name[1]
            effective_dev_prog_dict.setdefault(prog,[]).append(dev)
    #print effective_dev_prog_dict[effective_dev_prog_dict.keys()[0]]
    return effective_dev_prog_dict

def analyse_loyalty(prog_weekly_dev_dict_list, dma_code, mso):
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
        #sort_col_name.append('t%s-%s' %(str(i-1),str(i)))
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
        prog_list.append(value)

    df = pd.DataFrame(prog_list)
    df = df.fillna(0)
    #df = df.sort(sort_col_name, ascending=[0,0,0])
    df['viewer_mean'] = df[sort_col_name].mean(axis=1)
    df = df.sort('viewer_mean',ascending=0)
    col_name.sort(key=lambda x:len(x))
    df.to_csv('output_dma=%s_mso=%s.csv'%(dma_code,mso), columns=['program_title']+col_name)

def count_dev_per_prog(dma_code, mso):
    db = fwm_config.Database()

    filtered_re = get_prog_viewtime_matrix(db, '04', dma_code, mso)
    filtered_re.extend(get_prog_viewtime_matrix(db, '05', dma_code, mso))
    filtered_re.extend(get_prog_viewtime_matrix(db, '06', dma_code, mso))

    df = pd.DataFrame(filtered_re)
    #return
    #df = df[df.program_tms_id != 'MP_MISSINGPROGRAM'].reset_index(drop=True)
    df = df[(df.program_tms_id != 'MP_MISSINGPROGRAM') & (df.program_tms_id is not None)]
    df.loc[df.program_title=='2014 NBA Finals','program_title'] = 'NBA Basketball'
    df['program_viewtime'] = df['program_viewtime'].map(viewtime_threshold)
    df['week'] = df['event_date'].map(lambda x : x.isocalendar()[1])
    df = df[(df.week >= 14) & (df.week <= 26)]
    #print df.head()
    #print df.tail()
    #week_groups = df.groupby('week').groups
    #prog_weekly_dev_dict_list = map(lambda x : count_dev_per_prog_weekly(df.iloc[x,:]), week_dict.values())
    prog_weekly_dev_dict_list = map(lambda x : count_dev_per_prog_weekly(x[1]), df.groupby('week'))
    analyse_loyalty(prog_weekly_dev_dict_list, dma_code, mso)

if __name__=='__main__':
    # dma = 524 (Atlanta); 501 (New York)
    #tasks = [(-1,6760),(524,-1),(501,-1),(-1,6330)] # (dma_code, mso)
    tasks = [(524,-1),(501,-1)] # (dma_code, mso)
    for task in tasks:
        print 'Start to analyze task ', task, ' at: ', datetime.now()
        count_dev_per_prog(task[0], task[1])
    print 'All Done at: ', datetime.now()
