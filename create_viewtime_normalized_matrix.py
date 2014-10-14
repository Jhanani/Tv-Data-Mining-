import sys
import pandas as pd
import numpy as np
from datetime import date,time,datetime,timedelta
import calendar
from loyalty_analysis import viewtime_threshold
import fwm_config as fwm_config

def get_prog_timeslots_within_month(db,start_date,end_date,dma_code,mso,prime_start=20):
    if start_date.month<4:
        return []
    if start_date.month<10:
        month = '0%s' %start_date.month
    else:
        month = start_date.month
    db_tv_table = 'fwm_tv_%s_2014' %month
    db_ref_table = 'fwm_ref_%s_2014' %month
    print 'db_tv_table name:',db_tv_table
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
                SELECT event_date, event_time, program_title FROM {0}
                WHERE device_id IN %s AND event_date BETWEEN %s AND %s
                AND event_id IS NOT NULL AND program_title IS NOT NULL
                AND event_time BETWEEN %s AND %s;
                """.format(db_tv_table)
    print 'dev_list length:',len(dev_list)
    print 'start_date and end_date:',start_date,end_date
    result = db.query_tuple(query_data, (dev_list,start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'),str(prime_start_time),str(prime_end_time)))
    print 'result count ', len(result)
    prog_timeslot_dict = dict()
    for entry in result:
        prog_title = entry['program_title'].replace(',',' ')
        #if entry['program_title'] == "'80s Rewind Festival 2013":
        #    print entry
        #    print prog_title
        if entry['program_title'] == '2014 NBA Finals':
            prog_title = 'NBA Basketball'
        prog_timeslot_dict.setdefault(prog_title,set()).add(entry['event_date'].strftime('%Y-%m-%d')+'-'+str(entry['event_time'].seconds/1800))
    #print result[0]
    #print result[1]
    #print result[0]['event_time'].seconds / (30*60)
    #print prog_timeslot_dict.items()[0]
    return prog_timeslot_dict

def create_viewtime_matrix(dma,mso,start_date,end_date,prime_start):
    print 'start_date:',start_date
    print 'end_date:',end_date
    db = fwm_config.Database()
    if end_date.month != start_date.month:
        lastday = calendar.monthrange(2014,start_date.month)[1]
        end_date_1 = date(2014,start_date.month,lastday)
        start_date_1 = date(2014,end_date.month,1)
        prog_timeslots = get_prog_timeslots_within_month(db,start_date,end_date_1,dma,mso,prime_start)
        prog_timeslots1 = get_prog_timeslots_within_month(db,start_date_1,end_date,dma,mso,prime_start)
        for prog in prog_timeslots1:
            prog_timeslots[prog] = prog_timeslots.setdefault(prog,set()).union(prog_timeslots1[prog])
    else:
        prog_timeslots = get_prog_timeslots_within_month(db,start_date,end_date,dma,mso,prime_start)
    #start_date0 = date(2014,4,1)
    #end_date0 = date(2014,4,30)
    #start_date1 = date(2014,5,1)
    #end_date1 = date(2014,5,31)
    #start_date2 = date(2014,6,1)
    #end_date2 = date(2014,6,30)
    #prog_timeslots0 = get_prog_timeslots_within_month(db,start_date0,end_date0,dma,mso,prime_start)
    #prog_timeslots1 = get_prog_timeslots_within_month(db,start_date1,end_date1,dma,mso,prime_start)
    #prog_timeslots2 = get_prog_timeslots_within_month(db,start_date2,end_date2,dma,mso,prime_start)
    ##print prog_timeslots0['The Big Bang Theory']
    #for prog in prog_timeslots1:
    #    prog_timeslots0[prog] = prog_timeslots0.setdefault(prog,set()).union(prog_timeslots1[prog])
    #for prog in prog_timeslots2:
    #    prog_timeslots0[prog] = prog_timeslots0.setdefault(prog,set()).union(prog_timeslots2[prog])
    ##print prog_timeslots0['The Big Bang Theory']

    #print 'Program timeslots dict:'
    #print prog_timeslots
    viewtime_file = './output_viewtime_matrix_%s_%s_%s.csv'%(start_date.month,start_date.day,dma)
    df = pd.read_csv(viewtime_file, index_col=0)
    #print df.head(3)
    #print prog_timeslots
    for prog in df.columns:
        try:
            #print prog_timeslots[prog]
            if len(prog_timeslots[prog])==0:
                print 'error'
                return
            df[prog] /= len(prog_timeslots[prog])
        except KeyError:
            print 'key error:',prog
            return

    #dev_list = df.index.tolist()
    #df = df.apply(normalize_viewtime, axis=0, args=(timeslots_dict,))
    df.to_csv('output_viewtime_normalized_matrix_%s_%s_%s.csv'%(start_date.month,start_date.day,dma))

if __name__=='__main__':
    start_date = date(2014,6,9)
    end_date = date(2014,6,29)
    #create_viewtime_matrix(501,-1,date(2014,4,1),date(2014,6,30),20)
    #create_viewtime_matrix(618,-1,date(2014,4,1),date(2014,6,30),19)

    #sys.exit()

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
