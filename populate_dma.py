import MySQLdb

import fwm_config as fwm_config

config = fwm_config.Config()

from datetime import datetime, timedelta, date
#from insert_programview2db_mysqldb_new.py import 

import os
import sys

import xml.etree.ElementTree as ET

months_with_30_days = ['Apr', 'Jun', 'Sep', 'Nov']
months_with_31_days = ['Jan', 'Mar', 'May', 'Jul', 'Aug', 'Oct', 'Dec']

BULK_INSERT_MAX_COUNT = 10000

def date_str(year, month, i):
	date = year + '-' + month + '-'
	if i < 10:
		return date + str(0) + str(i)
	else:
		return date + str(i)

def process_dma_file(dma_file, dma_set):
	tree = ET.parse(dma_file)
	root = tree.getroot()
	for child in root:
		mso = child.attrib['mso']
		mso_name = child.attrib['mso-name']
		for item in child:
			device_id = item.attrib['device-id']
			household_id = item.attrib['household-id']
			household_type = item.attrib['household-type']
			dma_name = item.attrib['dma']
			system_type = item.attrib['system-type']
			try:
				zipcode = item.attrib['zipcode']
			except KeyError:
				zipcode = -1
			try:
				dma_code = item.attrib['dma-code']
			except KeyError:
				dma_code = -1
			ref_info = [mso_name,household_type,zipcode,dma_name,system_type]
			if (mso,device_id,dma_code,household_id) in dma_set:
				#if dma_set[(mso,device_id)][3] is None and ref_info[3] is not None:
				if dma_set[(mso,device_id,dma_code,household_id)] != ref_info:
					dma_set[(mso,device_id,dma_code,household_id)] = ref_info
					print 'error!',mso,';',device_id,';',dma_code
			#	if dma_set[(mso,device_id)][5] != ref_info[5]:
			#		dma_set[(mso,device_id)][5] = ref_info[5]
			#		dma_set[(mso,device_id)][4] = ref_info[4]
			#		print 'one dma info updated!',mso,';',device_id,';',ref_info[5]
			else:
				dma_set[(mso,device_id,dma_code,household_id)]=ref_info
			#try:
			#	device_id = item.attrib['device-id']
			#	household_id = item.attrib['household-id']
			#	household_type = item.attrib['household-type']
			#	zipcode = item.attrib['zipcode']
			#	dma_name = item.attrib['dma']
			#	dma_code = item.attrib['dma-code']
			#	system_type = item.attrib['system-type']
			#	ref_info = (mso,mso_name,device_id,household_id,\
			#			household_type,zipcode,dma_name,dma_code,system_type)
			#	#print ref_info
			#	dma_set.add(ref_info)
			#except KeyError:
			#	print 'KeyError', item.attrib
			#break
		#break

def mysqldb_bulk_insert_into_fwm_ref(db, db_table_name, mysql_data):

	# Insert query
	query = """
		INSERT INTO {0}
		(mso, mso_name, device_id, household_id, household_type, zipcode,\
				dma_name, dma_code, system_type)
		VALUES
		(%s, %s, %s, %s, %s, %s, %s, %s, %s);
		""".format(db_table_name)

	#print query
	#print mysql_data
	db.bulk_insert_no_commit(query, mysql_data)

def bulk_insert2db(dma_dict, db, db_table_name):
	#dma_list = map(list, dma_dict)
	mysql_bulk_data = list()
	for key,value in dma_dict.items():
		mysql_data = list()
		mysql_data.append(int(key[0]))
		mysql_data.append(value[0])
		mysql_data.append(key[1])
		mysql_data.append(key[3])
		mysql_data.append(value[1])
		mysql_data.append(int(value[2]))
		mysql_data.append(value[3])
		mysql_data.append(int(key[2]))
		mysql_data.append(value[4])

		mysql_bulk_data.append(mysql_data)

		if len(mysql_bulk_data) == BULK_INSERT_MAX_COUNT:
			print 'Inserting into database ', len(mysql_bulk_data), ' rows'
			mysqldb_bulk_insert_into_fwm_ref(db, db_table_name, mysql_bulk_data)
			mysql_bulk_data = list()

			db.explicit_commit()

	if len(mysql_bulk_data):
		print 'Inserting into database ', len(mysql_bulk_data), ' rows'
		mysqldb_bulk_insert_into_fwm_ref(db, db_table_name, mysql_bulk_data)
	db.explicit_commit()

def populate_dma(curr_month):

	curr_year = '2014'

	db_table_name = 'fwm_ref_' + curr_month + '_' + curr_year
	db = fwm_config.Database()

	dma_dir = '/files2/Temp/fwm_'+curr_month+'/refxml/'
	dma_file_prefix = '4Cinsights.rpt_refxml.date_'
	dma_file_suffix = '.xml'

	start_date = datetime.strptime(curr_year+curr_month+'01', '%Y%m%d')
	start = 1
	end = 28
	if start_date.strftime('%b') in months_with_30_days:
		end = 30
	elif start_date.strftime('%b') in months_with_31_days:
		end = 31
	print end
	#end = 2
	dma_dict = dict()
	print 'Beginning Processing DMA Files ...'
	for i in range(start, end):
		dma_filename = dma_file_prefix
		dma_filename += date_str(curr_year, curr_month, i) + dma_file_suffix

		dma_abs_file_path = os.path.join(os.path.sep,dma_dir,dma_filename)
		#print dma_abs_file_path
		
		if os.path.isfile(dma_abs_file_path):
			print 'Starting to analyze file: ', dma_abs_file_path, ' at: ', datetime.now()
			process_dma_file(dma_abs_file_path, dma_dict)
			print 'size of dma_set:', len(dma_dict)
		else:
			print 'Error: File doesnt exists, ', dma_abs_file_path
	print 'total size of dma_set:', len(dma_dict)
	bulk_insert2db(dma_dict, db, db_table_name)
	print 'All DONE ...'


if __name__ == '__main__':
	populate_dma('04')
	populate_dma('05')
	populate_dma('06')
