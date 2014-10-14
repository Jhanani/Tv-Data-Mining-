import sys
import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta
import calendar
from daily_loyalty import get_prog_viewtime_matrix_within_month
from loyalty_analysis import viewtime_threshold
import fwm_config as fwm_config

def create_viewtime_matrix(dma,mso,start_date,end_date,prime_start):
    print 'start_date:',start_date
    print 'end_date:',end_date
    db = fwm_config.Database()
    if end_date.month != start_date.month:
        lastday = calendar.monthrange(2014,start_date.month)[1]
        end_date_1 = date(2014,start_date.month,lastday)
        start_date_1 = date(2014,end_date.month,1)
        print end_date_1,start_date_1
        viewtime_data = get_prog_viewtime_matrix_within_month(db,start_date,end_date_1,dma,mso,prime_start)
        viewtime_data.extend(get_prog_viewtime_matrix_within_month(db,start_date_1,end_date,dma,mso,prime_start))
    else:
        viewtime_data = get_prog_viewtime_matrix_within_month(db,start_date,end_date,dma,mso,prime_start)
    #start_date0 = date(2014,4,1)
    #end_date0 = date(2014,4,30)
    #start_date1 = date(2014,5,1)
    #end_date1 = date(2014,5,31)
    #start_date2 = date(2014,6,1)
    #end_date2 = date(2014,6,30)
    #viewtime_data = get_prog_viewtime_matrix_within_month(db,start_date0,end_date0,dma,mso,prime_start)
    #viewtime_data.extend(get_prog_viewtime_matrix_within_month(db,start_date1,end_date1,dma,mso,prime_start))
    #viewtime_data.extend(get_prog_viewtime_matrix_within_month(db,start_date2,end_date2,dma,mso,prime_start))
    data_df = pd.DataFrame(viewtime_data)
    
    data_df.loc[data_df.program_title=='2014 NBA Finals','program_title'] = 'NBA Basketball'
    data_df['program_viewtime'] = data_df['program_viewtime'].map(viewtime_threshold)
    dev_num = np.unique(data_df['device_id']).size
    print 'total device number is ',dev_num
    viewtime_matrix = dict()
    for prog_dev, group in data_df.groupby(['program_title','device_id']):
        viewtime = group['program_viewtime'].sum()
        viewtime_matrix.setdefault(prog_dev[0],{})[prog_dev[1]] = (viewtime > 0 and viewtime or 0)
    re_df = pd.DataFrame(viewtime_matrix)
    re_df = re_df.fillna(0)
    print re_df.shape
    #re_df = re_df[re_df.sum(axis=1) > 15*60*90]
    re_df = re_df[re_df.sum(axis=1) > 15*60*7]
    print re_df.shape
    filtered_prog = [col for col in re_df.columns if np.count_nonzero(re_df[col]) > dev_num*0]
    re_df = re_df[filtered_prog]
    #re_df = re_df.transpose()
    #re_df = [row for row in re_df.values if np.count_nonzero(row)>dev_num*0.01]
    #re_df = pd.DataFrame(re_df)
    #re_df = re_df.transpose()
    re_df = re_df[(re_df.T != 0).any()]
    re_df = re_df[re_df.columns[(re_df!=0).any()]]
    re_df.rename(columns=lambda x:x.replace(',',' '), inplace=True)
    print 'size of matrix is:',re_df.shape
    re_df.to_csv('output_viewtime_matrix_%s_%s_%s.csv'%(start_date.month,start_date.day,dma), sep=',')

if __name__=='__main__':
    start_date = date(2014,4,1)
    end_date = date(2014,6,30)
    create_viewtime_matrix(501,-1,date(2014,6,9),date(2014,6,15),20)
    #create_viewtime_matrix(618,-1,date(2014,4,1),date(2014,6,30),19)

    sys.exit()

    iter_date = start_date
    while iter_date <= end_date:
        print 'Start analyzing week:',iter_date,' at ',datetime.now()
        week_end = iter_date + timedelta(days=6-iter_date.weekday())
        if week_end > end_date:
            create_viewtime_matrix(618,-1,iter_date,end_date,19)
            create_viewtime_matrix(501,-1,iter_date,end_date,20)
        else:
            create_viewtime_matrix(618,-1,iter_date,week_end,19)
            create_viewtime_matrix(501,-1,iter_date,week_end,20)
        iter_date += timedelta(days=7-iter_date.weekday())
    print 'All done at ',datetime.now()
