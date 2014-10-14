import sys
import MySQLdb
import fwm_config as fwm_config
import numpy as np
import pandas as pd
from scipy.stats import itemfreq

itemset_file = './utility_mining/frequent_utility_itemset.txt'
prog_name_file = './utility_mining/utility_mine_program_file.txt'
#prog_view_file = './output_viewtime_normalized_matrix_4_1_618_all.csv'

#MAX_UTILITY_VAL = 11251685
#MAX_UTILITY_VAL = 85703749
#MAX_UTILITY_VAL = 1972966

def cal_supp(itemset, prog_view_df):
    return prog_view_df[itemset][prog_view_df[itemset]!=0].dropna().shape[0]*1.0/prog_view_df.shape[0]

def count_freq(itemset, prog_view_df, f, row_header, prog_in_set):
    #total_num = prog_view_df.shape[0]
    ##combined_freq = len( prog_view_df[itemset][prog_view_df[itemset]!=0].dropna().index )
    #combined_freq = prog_view_df[itemset][prog_view_df[itemset]!=0].dropna().shape[0]
    ##freq_list = prog_view_df[itemset][prog_view_df[itemset]!=0].count()
    #freq_list = (prog_view_df[itemset]!=0).sum()
    #product = 1
    #for i in range(0,len(freq_list)):
    #	confidence = combined_freq*1.0/freq_list[i]
    #	product *= freq_list[i]
    #	print itemset[i], ' confidence:', confidence
    #print 'lift:',combined_freq*total_num*1.0/product
    
    #test_df = prog_view_df[itemset][prog_view_df[itemset]!=0].dropna(how='all')
    combined_supp = cal_supp(itemset,prog_view_df)
    
    #for l in range(1, len(itemset)):
    #	for i in range(0, len(itemset)-l+1):
    #		indices = range(i,i+l)
    #		print 'rule:',itemset[i:i+l],' -> ',[v for j,v in enumerate(itemset) if j not in indices],
    #		supp = cal_supp(itemset[i:i+l],prog_view_df)
    #		confidence = combined_supp*1.0/supp
    #		lift = confidence*1.0/cal_supp([v for j,v in enumerate(itemset) if j not in indices],prog_view_df)
    #		print 'confidence',confidence,
    #		print 'lift',lift
    
    for i in range(0, len(itemset)):
        f.write(row_header)
        indices = range(i,i+1)
        f.write(';'.join([v for j,v in enumerate(prog_in_set) if j not in indices])+',')
        f.write(prog_in_set[i]+',')
        supp = cal_supp([v for j,v in enumerate(itemset) if j not in indices],prog_view_df)
        confidence = combined_supp*1.0/supp
        lift = confidence*1.0/cal_supp(itemset[i:i+1],prog_view_df)
        f.write(str(confidence)+',')
        f.write(str(lift)+'\n')

def itemset_analysis(db, prog_view_file):
    #dma = 618
    with open(itemset_file,'rb') as item_file:
        lines = item_file.read().splitlines()
    itemset_list = [line.strip('\n\t\r ').replace(' ','').split(',') for line in lines]
    itemset_list.sort(key = lambda x : float(x[-1]), reverse = True)
    
    with open(prog_name_file,'rb') as prog_file:
        lines = prog_file.read().splitlines()
    prog_list = [line.strip('\n\t\r ').split(',')[1] for line in lines]
    
    prog_view_df = pd.read_csv(prog_view_file)
    dev_list = prog_view_df.ix[:,0].values
    prog_view_df.drop(prog_view_df.columns[0],axis=1,inplace=True)
    #print prog_view_df.head()
    
    f = open('utility_results_%s'%prog_view_file,'w')
    f.write('Utility_value,Utility_fraction,Program_name,Rule_from,Rule_to,Confidence,Lift\n')
    for i in range(0,min(1000,len(itemset_list))):
        itemset = itemset_list[i]
        utility_fraction = float(itemset.pop())
        utility_value = float(itemset.pop())
        row_header = str(utility_value)+','+str(utility_fraction)+','
        #row_header += ' '.join(itemset)+','
        itemset = [int(item) for item in itemset]
        prog_in_set = [prog_list[i] for i in itemset]
        row_header += ';'.join(prog_in_set)+','
        count_freq(itemset, prog_view_df, f, row_header, prog_in_set)
        #dev_indices = prog_view_df[itemset][prog_view_df[itemset]!=0].dropna().index.values
        #print dev_indices
        #dev_in_cluster = dev_list[dev_indices]
        #print dev_in_cluster
        #print 'number of devices within this cluster:',len(dev_in_cluster)
        #get_income(db,dma,dev_in_cluster.tolist())

def get_income(db,dma,dev_list):
    db_ref_table = 'fwm_ref_04_2014'
    db_demo_table = 'fwm_demo_04_2014'
    query_household = """
                    SELECT household_id FROM {0}
                    WHERE device_id IN %s AND dma_code = %s;
                    """.format(db_ref_table)
    household_ids = db.query_tuple(query_household, (dev_list, dma) )
    household_id_list = map(lambda x : x.values()[0], household_ids)
    #household_id_list = map(lambda x : x.values(), household_ids)
    #household_id_list = [item for sublist in household_id_list for item in sublist]
    household_id_len = len(household_id_list)
    print 'Number of household_ids is:',household_id_len
    #print household_id_list
    #return
    
    query_income = """
                    SELECT hh_income FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    incomes = db.query_new(query_income, household_id_list)
    income_list = map(lambda x : x.values()[0], incomes)
    income_list = [x for x in income_list if x is not None]
    #income_array = np.array(income_list)
    #print 'freq',itemfreq(income_list)
    return income_list

if __name__=='__main__':
    db = fwm_config.Database()
    itemset_analysis(db, sys.argv[1])

