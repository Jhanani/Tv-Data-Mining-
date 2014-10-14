import pandas as pd
import numpy as np
from datetime import date,datetime,timedelta

from operator import add

from matplotlib import pyplot as plt

from sklearn.datasets import make_biclusters
from sklearn.datasets import samples_generator as sg
from sklearn.cluster.bicluster import SpectralCoclustering, SpectralBiclustering
from sklearn.metrics import consensus_score
import matplotlib.colors as colors

import random
from sklearn.decomposition import ProjectedGradientNMF
import heapq

import fwm_config as fwm_config

# Mins*7.0*days
LOW_LIMIT = 15*60*7.0
#HIGH_LIMIT = 2*60*60*90.0
#LOW_LIMIT = 2*60*60*90.0 + 1.0
HIGH_LIMIT = 24*60*60*7.0

def get_demo_info(db, dma, dev_list, info_name):
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
    if info_name == 'child_present':
        query_info = """
                    SELECT children_present FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    elif info_name == 'income':
        query_info = """
                    SELECT hh_income FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    elif info_name == 'age':
        query_info = """
                    SELECT hh_age_range FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    info = db.query_new(query_info, household_id_list)
    info_list = map(lambda x : x.values()[0], info)
    info_list = [x for x in info_list if x is not None]
    #income_array = np.array(income_list)
    #print 'freq',itemfreq(income_list)
    return info_list

class all_clusters:
    db = fwm_config.Database()
    def generate_clusters(self):
        cluster_list = list()
        for i in range(0, self.cluster_num):
            dev_indices = [index for index,v in enumerate(self.membership) if v==i]
            dev_in_cluster = self.device_list[dev_indices]
            #yield prog_cluster(i, self.dma, dev_in_cluster)
            cluster_list.append(prog_cluster(i, self.dma, dev_in_cluster))
        return cluster_list
    def __init__(self, dma, cluster_num ,devices ,membership):
        self.dma = dma
        self.cluster_num = cluster_num
        self.device_list = devices
        self.membership = membership
        self.cluster_list = self.generate_clusters()
    def clusters_match(self, clusters_obj):
        intersection = list(set(self.device_list) & set(clusters_obj.device_list))
        print 'device numbers:',len(self.device_list),' ',len(clusters_obj.device_list)
        print 'common device number between this two weeks:',len(intersection)
        print 'common/week1 = ',len(intersection)*1.0/len(self.device_list),'; common/week2 = ',len(intersection)*1.0/len(clusters_obj.device_list)
        for cluster1 in self.cluster_list:
            for cluster2 in clusters_obj.cluster_list:
                cluster1.match_cluster(cluster2,intersection)

class prog_cluster:
    db = fwm_config.Database()
    def __init__(self,cluster_id,dma_code,devices):
        self.id = cluster_id
        self.dma = dma_code
        self.dev_list = devices
    def match_cluster(self, cluster, common_dev_list):
        intersection = list(set(self.dev_list) & set(cluster.dev_list) & set(common_dev_list))
        effective_self_dev_list = list(set(self.dev_list) & set(common_dev_list))
        effective_cluster_dev_list = list(set(cluster.dev_list) & set(common_dev_list))
        common_self = len(intersection)*1.0/len(effective_self_dev_list)
        common_cluster = len(intersection)*1.0/len(effective_cluster_dev_list)
        if common_self > 0.1 or common_cluster > 0.1:
            print 'match results between cluster%s and cluster%s:'%(self.id,cluster.id)
            print 'common = ', len(intersection)
            print 'cluster%s = '%self.id, len(effective_self_dev_list)
            print 'cluster%s = '%cluster.id, len(effective_cluster_dev_list)
            print 'common / cluster%s = '%self.id, common_self
            print 'common / cluster%s = '%cluster.id, common_cluster

def age_analysis(dma, dev_id_list, cluster_num, membership):
    db = fwm_config.Database()
    all_age_info = get_demo_info(db, dma, dev_id_list.tolist(), 'age')
    total_num = len(all_age_info)
    print 'number of all age info',total_num
    all_age_info = [map(int,list(item)) for item in all_age_info]
    #print all_age_info[0]
    accumulated_info = all_age_info[0]
    for i in range(1,len(all_age_info)):
        accumulated_info = map(add, accumulated_info, all_age_info[i])
    #print accumulated_info
    male_age_distr = map(str,accumulated_info[0::3])
    female_age_distr = map(str,accumulated_info[1::3])
    unknown_age_distr = map(str,accumulated_info[2::3])
    #print male_age_distr
    #print female_age_distr
    #print unknown_age_distr
    file = open('decompose_results_age_%s.csv'%dma,'w')
    file.write('Cluster,Gender,Age_18_24,Age_25_34,Age_35_44,Age_45_54,Age_55_64,Age_65_74,Age_75\n')
    file.write('-1,male,%s\n'%(','.join(male_age_distr)))
    file.write('-1,female,%s\n'%(','.join(female_age_distr)))
    file.write('-1,unknown,%s\n'%(','.join(unknown_age_distr)))
    for i in range(0, cluster_num):
        dev_indices = [index for index,v in enumerate(membership) if v==i]
        dev_in_cluster = dev_id_list[dev_indices]
        age_info = get_demo_info(db,dma,dev_in_cluster.tolist(),'age')
        age_info = [map(int,list(item)) for item in age_info]
        accumulated_info = age_info[0]
        for j in range(1,len(age_info)):
            accumulated_info = map(add,accumulated_info,age_info[j])
        male_age_distr = map(str,accumulated_info[0::3])
        female_age_distr = map(str,accumulated_info[1::3])
        unknown_age_distr = map(str,accumulated_info[2::3])
        file.write('%s,male,%s\n'%(i,','.join(male_age_distr)))
        file.write('%s,female,%s\n'%(i,','.join(female_age_distr)))
        file.write('%s,unknown,%s\n'%(i,','.join(unknown_age_distr)))

def child_present_analysis(dma,dev_id_list,cluster_num,membership):
    db = fwm_config.Database()
    all_child_present = get_demo_info(db, dma, dev_id_list.tolist(), 'child_present')
    total_num = len(all_child_present)
    print 'number of all children info',total_num
    all_child_freq = dict()
    file = open('decompose_results_child_%s.csv'%dma,'w')
    file.write('Cluster,Children_present,Num_dev,Fraction_in_cluster,Fraction_overall\n')
    for item in all_child_present:
        try:
            all_child_freq[item]+=1
        except KeyError:
            all_child_freq[item]=1
    for key in sorted(all_child_freq):
        file.write('-1,%s,%s,%s,\n'%(key,all_child_freq[key],all_child_freq[key]*1.0/total_num))
    for i in range(0, cluster_num):
        dev_indices = [index for index,v in enumerate(membership) if v==i]
        dev_in_cluster = dev_id_list[dev_indices]
        child_present = get_demo_info(db, dma, dev_in_cluster.tolist(),'child_present')
        child_info_num = len(child_present)
        child_freq = dict()
        for item in child_present:
            try:
                child_freq[item]+=1
            except KeyError:
                child_freq[item]=1
        for key in sorted(child_freq):
            file.write('%s,%s,%s,%s,%s\n'%(i,key,child_freq[key],child_freq[key]*1.0/child_info_num,child_freq[key]*1.0/all_child_freq[key]))
    file.close()

def income_analysis(dma,dev_id_list,cluster_num,membership):
    db = fwm_config.Database()
    all_income = get_demo_info(db, dma, dev_id_list.tolist(), 'income')
    total_num = len(all_income)
    print 'number of all income info',total_num
    all_income_freq = dict()
    file = open('decompose_results_income_%s.csv'%dma,'w')
    file.write('Cluster,Income_level,Num_dev,Fraction_in_cluster,Fraction_overall\n')

    for item in all_income:
        try:
            all_income_freq[item]+=1
        except KeyError:
            all_income_freq[item]=1

    for key in sorted(all_income_freq):
        #print key,' ',all_income_freq[key],' ',all_income_freq[key]*1.0/total_num
        file.write('-1,%s,%s,%s,\n'%(key,all_income_freq[key],all_income_freq[key]*1.0/total_num))

    for i in range(0, cluster_num):
        dev_indices = [index for index,v in enumerate(membership) if v==i]
        #print dev_indices
        dev_in_cluster = dev_id_list[dev_indices]
        #print 'number of devices in this cluster:',len(dev_in_cluster)
        incomes = get_demo_info(db,dma,dev_in_cluster.tolist(),'income')
        income_num = len(incomes)
        income_freq = dict()
        for item in incomes:
            try:
                income_freq[item]+=1
            except KeyError:
                income_freq[item]=1
        for key in sorted(income_freq):
            #print key,' ',income_freq[key],' ',income_freq[key]*1.0/income_num,' ',
            #print income_freq[key]*1.0/all_income_freq[key]
            file.write('%s,%s,%s,%s,%s\n'%(i,key,income_freq[key],income_freq[key]*1.0/income_num,income_freq[key]*1.0/all_income_freq[key]))

    file.close()

def get_household_num(dma, dev_list):
    db = fwm_config.Database()
    db_ref_table = 'fwm_ref_04_2014'
    db_demo_table = 'fwm_demo_04_2014'
    query_household = """
                    SELECT household_id FROM {0}
                    WHERE device_id IN %s AND dma_code = %s;
                    """.format(db_ref_table)
    household_ids = db.query_tuple(query_household, (dev_list, dma) )
    household_id_list = map(lambda x : x.values()[0], household_ids)
    household_id_set = set(household_id_list)
    return len(household_id_set)

# Trying the NMF
def filter_1sigma_nmf_new(dma, iter_date, df, header_df):
    print 'Get the 1-sigma filtered data'
    print df.shape[1]
    idx_vt = df.shape[1]-1
    mean_viewtime = df[idx_vt].mean()
    std_viewtime = df[idx_vt].std()

    print mean_viewtime/3600.0, std_viewtime/3600.0

    reduced_df = df[(df[idx_vt] >= LOW_LIMIT)&(df[idx_vt] <= HIGH_LIMIT)].reset_index()
    print reduced_df.shape

    reduced_df[range(1,idx_vt)] = reduced_df[range(1,idx_vt)].div(1.0*reduced_df[idx_vt], 'index')
    dev_id_list = reduced_df[0]

    reduced_df_vsum = reduced_df[range(1, idx_vt)].sum()
    reduced_df_vsum = reduced_df_vsum[reduced_df_vsum > 0.00]
    idx_list = reduced_df_vsum.index.tolist()
    reduced_df_1 = reduced_df[range(1, idx_vt)][reduced_df_vsum.index.tolist()]

    # Select the header accordingly
    reduced_header_df = header_df[idx_list]

    #program_viewtime_array = np.array(reduced_df[range(1,idx_vt)].astype(np.float))
    program_viewtime_array = np.array(reduced_df_1.astype(np.float))
    program_name_array = np.array(reduced_header_df)

    t_program_viewtime_array = program_viewtime_array.transpose()

    cluster_num = 14
    # Non-negative Matrix Factorization
    model = ProjectedGradientNMF(n_components = cluster_num, sparseness='data', init='nndsvd', max_iter=400, random_state=0)
    WW = model.fit_transform(t_program_viewtime_array)
    t_WW = WW.transpose()
    HH = model.components_
    t_HH = HH.transpose()
    #print t_HH.shape
    #print pd.DataFrame(t_HH).head()
    membership = [-1 for item in range(0, t_HH.shape[0])]
    # Assign the membership
    for i in range(0, t_HH.shape[0]):
        membership[i] = np.argmax(t_HH[i])

    dd = reduced_header_df
    print dd.shape
    print program_name_array.shape
    print program_viewtime_array.shape

    file = open('decompose_results_clusters_%s_%s_%s.csv'%(iter_date.month, iter_date.day, dma),'w')
    file.write('Cluster_id,Dev_num,Household_num,Feature_val,Feature_fraction,Program_name\n')
    file.write('-1,%s,%s,,,\n'%(len(dev_id_list),get_household_num(dma,dev_id_list.tolist())))
    cluster_num = t_WW.shape[0]

    for i in range(0, cluster_num):
        dev_indices = [index for index,v in enumerate(membership) if v==i]
        dev_in_cluster = dev_id_list[dev_indices]
        dev_num = len(dev_in_cluster)
        household_num = get_household_num(dma,dev_in_cluster.tolist())

        #print heapq.nlargest(10,t_WW[i])
        feature_val = np.sort(t_WW[i])
        feature_val = feature_val[::-1]
        #print 't_WW:',t_WW[i]
        #print 'sorted t_WW:',feature_val
        val_sum = np.sum(feature_val)
        feature_frac = feature_val*1.0/val_sum
        accumulated_frac = 0
        cut_ind = 0
        for frac in feature_frac:
            accumulated_frac += frac
            cut_ind += 1
            if accumulated_frac > 0.6:
                break
        idx_list = np.argsort(t_WW[i])[::-1][:cut_ind]
        program_list = program_name_array[0][idx_list]
        for j in range(0,cut_ind):
            file.write('%s,%s,%s,%s,%s,%s\n'%(i,dev_num,household_num,feature_val[j],feature_frac[j],program_list[j]))
        #file.write(' '.join(program_name_array[0][idx_list]))
        #file.write('\n')
    file.close()
    #income_analysis(dma, dev_id_list, cluster_num, membership)
    #child_present_analysis(dma, dev_id_list, cluster_num, membership)
    #age_analysis(dma, dev_id_list, cluster_num, membership)
    clusters_obj = all_clusters(dma, cluster_num ,dev_id_list ,membership)
    return clusters_obj

def clustering_analysis(dma):
    print 'This is the main program'
    fp = open('./output_viewtime_matrix_full_%s.csv'%dma)
    for line in fp:
        # Notice we are not getting rid of the first column
        header = line.strip('\t\r\n\ ').split(',')
        break
    print 'Number of programs : ', len(header)
    header_df = pd.DataFrame([header])
    fp.close()

    data = pd.read_csv('./output_viewtime_matrix_full_%s.csv'%dma)
    #data.reset_index(drop=True,inplace=True)
    col_name = data.columns.values.tolist()
    data.rename(columns = lambda x: col_name.index(x),inplace=True)

    myshape = data.shape
    data[myshape[1]] = data.sum(axis=1,numeric_only=True)
    #GOLD#new_data = data[[0, myshape[1]]]

    filter_1sigma_nmf_new(dma, data, header_df)

def clustering_analysis_weekly(dma, iter_date):
    print 'weekly clustering analysis'
    fp = open('./output_viewtime_matrix_%s_%s_%s.csv'%(iter_date.month, iter_date.day, dma))
    for line in fp:
        header = line.strip('\t\r\n\ ').split(',')
        break
    print 'Number of programs: ', len(header)
    header_df = pd.DataFrame([header])
    fp.close()
    data = pd.read_csv('./output_viewtime_matrix_%s_%s_%s.csv'%(iter_date.month, iter_date.day, dma))
    col_name = data.columns.values.tolist()
    data.rename(columns = lambda x: col_name.index(x),inplace=True)
    data[data.shape[1]] = data.sum(axis=1,numeric_only=True)
    clusters_obj = filter_1sigma_nmf_new(dma, iter_date, data, header_df)
    return clusters_obj

if __name__ == '__main__':
    start_date = date(2014,4,1)
    end_date = date(2014,4,8)
    iter_date = start_date
    clusters_obj_list = list()
    while iter_date <= end_date:
        print iter_date
        clusters_obj_list.append(clustering_analysis_weekly(501, iter_date))
        #clustering_analysis_weekly(501, iter_date)
        #clustering_analysis_weekly(618, iter_date)
        iter_date += timedelta(days=7-iter_date.weekday())
    #clustering_analysis(618)
    print 'Finish weekly analysis.'
    print 'Begin matching clusters:'
    cluster_obj1 = clusters_obj_list[0]
    for i in range(1,len(clusters_obj_list)):
        cluster_obj2 = clusters_obj_list[i]
        print 'match clusters between weeks:'
        cluster_obj1.clusters_match(cluster_obj2)
        cluster_obj1 = cluster_obj2

