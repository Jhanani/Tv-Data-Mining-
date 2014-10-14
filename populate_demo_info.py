import MySQLdb
import fwm_config as fwm_config

config = fwm_config.Config()

BULK_INSERT_MAX_COUNT = 1000

def mysqldb_bulk_insert(db, db_table_name, mysql_data):
	query = """
		INSERT INTO {0}
		(household_id, household_size, num_adults, num_generations,\
		hh_age_range, hh_head_marital_status, hh_head_race, children_present,\
		num_children, children_age, children_age_range, hh_head_dwelling_type,\
		home_owner_status, hh_head_residence_length, home_market_value, num_vehicles,\
		vehicle_make_code, vehicle_model, vehicle_year, hh_net_worth,\
		hh_income, hh_identify_gender, hh_identify_age, hh_identify_education,\
		hh_identify_occupation, hh_head_education, hh_head_occupation, hh_2nd_age,\
		hh_2nd_education, hh_2nd_occupation, hh_3rd_age, hh_3rd_education,\
		hh_3rd_occupation, hh_4th_age, hh_4th_education, hh_4th_occupation,\
		hh_5th_age, hh_5th_education, hh_5th_occupation, hh_head_political_party,\
		hh_head_voter_party, personicx_cluster_code, personicx_insurance_code, personicx_financial_code)
		VALUES
		(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
		%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
		%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
		%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
		%s, %s, %s, %s);
		""".format(db_table_name)
	db.bulk_insert_no_commit(query, mysql_data)

def populate_demo():
	db_table_name = 'fwm_demo_04_2014'
	db = fwm_config.Database()

	demo_dir = '/files2/Temp/demodata/'
	demo_file = '4Cinsights.rpt_demodata.20140113.pd'
	
	mysql_bulk_data = list()
	with open(demo_dir+demo_file) as f:
		for line in f:
			entry = line.strip('\n\t\r ').split('|')
			entry = [item if len(item)!=0 else None for item in entry]
			if entry[7]=='N':
				entry[7]=False
			elif entry[7]=='Y':
				entry[7]=True
			#print entry
			mysql_bulk_data.append(entry)
			if len(mysql_bulk_data) == BULK_INSERT_MAX_COUNT:
				print 'Inserting into databse ', len(mysql_bulk_data), ' rows'
				mysqldb_bulk_insert(db,db_table_name,mysql_bulk_data)
				mysql_bulk_data = list()
				db.explicit_commit()
			#break
		if len(mysql_bulk_data):
			print 'Inserting into database ', len(mysql_bulk_data), ' rows'
			mysqldb_bulk_insert(db,db_table_name,mysql_bulk_data)
		db.explicit_commit()

if __name__ == '__main__':
	populate_demo()
