import MySQLdb
import fwm_config as fwm_config

from numpy import genfromtxt
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.cluster.bicluster import SpectralCoclustering
from sklearn.cluster.bicluster import SpectralBiclustering
from scipy.stats import itemfreq

def fraction(row):
	return row*1.0/np.sum(row)

def biclustering(db):
	#mydata = genfromtxt('/home/fan/intern/process_db/analysis/viewtime_matrix_524.csv',dtype=None,delimiter=',',names=True,skip_header=1)
	df = pd.read_csv('/home/fan/intern/process_db/analysis/viewtime_matrix_501_0.1.csv')
	dma = 501
	#print df.head()
	print df.shape
	dev_list = df.ix[:,0].values
	prog_list = df.columns.values
	#print type(dev_list)
	#print type(prog_list)
	df.drop(df.columns[0],axis=1,inplace=True)
	#df[df==0] = 1
	df = df.apply(fraction,axis=1)
	#print df.head()
	#print df.values
	#print type(df.values)
	#mydata = df.values
	#mydata=np.delete(mydata, 0, axis=0)
	#mydata=np.delete(mydata, 0, axis=1)
	#mydata[mydata==0] = 0.01
	#print 'data format is:',mydata,type(mydata)
	# model=SpectralCoclustering(n_clusters=5, random_state=0)
	#n_clusters=(1000,20) # 4*3 = 12 clusters

	#model = SpectralBiclustering(random_state=None)
	model = SpectralCoclustering(n_clusters = 10)
	model.fit(df)
	#fit_data=mydata[np.argsort(model.row_labels_)]
	#fit_data=fit_data[:,np.argsort(model.column_labels_)]
	#plt.matshow(fit_data[0:40],cmap=plt.cm.Blues)
	# plt.show()
	print model.get_params()
	for i in range(0,5):
		print 'Size of one cluster:',model.get_shape(i)
		indices = model.get_indices(i)
		#print indices[1]
		print prog_list[indices[1]]
		print model.get_submatrix(i, df.values)
		dev_in_cluster = dev_list[indices[0]]
		#print type(dev_in_cluster)
		print 'number of devices within this cluster:',len(dev_in_cluster)
		get_income(db,dma,dev_in_cluster.tolist())

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
	print 'Number of household_ids is:',len(household_id_list)
	#print household_id_list
	#return

	query_income = """
				SELECT hh_income FROM {0} WHERE household_id IN %s;
				""".format(db_demo_table)
	incomes = db.query_new(query_income, household_id_list)
	income_list = map(lambda x : x.values()[0], incomes)
	#income_list = income_list[income_list!=None]
	#income_array = np.array(income_list)
	#print 'income level list:',income_list
	print 'freq',itemfreq(income_list)

if __name__=='__main__':
	db = fwm_config.Database()
	biclustering(db)
