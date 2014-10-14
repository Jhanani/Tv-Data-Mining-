import MySQLdb
import fwm_config as fwm_config

from datetime import date,datetime,timedelta

import pandas as pd

def get_total_viewtime_day(db, day, viewtime_dict):
	if day.month<10:
		month = '0%s' %day.month
	else:
		month = day.month
	db_table = 'fwm_tv_%s_2014' %month
	query = """
			SELECT SUM(program_viewtime) FROM {0} 
			WHERE event_date = %s AND event_id IS NOT NULL 
			AND event_time BETWEEN '19:00:00' AND '22:00:00';
			""".format(db_table)
	result = db.query_new(query, day.strftime("%Y-%m-%d"))
	viewtime_dict.setdefault(day.isocalendar()[1],{})[day.isoweekday()] = result[0].values()[0]

def weekly_pattern():
	db = fwm_config.Database()
	viewtime_dict = {}
	start_date = date(2014,4,1)
	end_date = date(2014,6,30)
	d = start_date
	delta = timedelta(days=1)
	while d <= end_date:
		print 'begin to analyze day %s'%d.strftime('%Y-%m-%d'), ' at:',datetime.now()
		get_total_viewtime_day(db, d, viewtime_dict)
		d += delta
	df = pd.DataFrame(viewtime_dict)
	df.to_csv('weekly_pattern.csv')

if __name__ == '__main__':
	weekly_pattern()
