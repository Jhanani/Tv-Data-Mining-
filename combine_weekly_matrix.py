import pandas as pd
import numpy as np

def combine_weekly_matrix():
    weeks = ["4_1","4_7","4_14","4_21","4_28","5_5","5_12","5_19","5_26","6_2","6_9","6_16","6_23","6_30"]
    df = pd.DataFrame()
    for week in weeks:
        weekly_file = 'output_viewtime_matrix_%s_501.csv'%week
        print 'start reading file:',weekly_file
        df_temp = pd.read_csv(weekly_file)
        df_temp = df_temp.rename(columns={'Unnamed: 0':'Device_id'})
        print 'df_temp shape',df_temp.shape
        df = df.append(df_temp)
        #df = df.groupby('Device_id').sum().fillna(0)
        print 'df shape',df.shape
    df = df.groupby('Device_id').sum().fillna(0)
    df.reset_index(['Device_id'])
    df.index.names=['']
    df = df[df.sum(axis=1) > 15*60*90]
    df = df[(df.T != 0).any()]
    df = df[df.columns[(df!=0).any()]]
    print df.shape
    df.to_csv('combined_viewtime_matrix_501.csv')

if __name__=='__main__':
    combine_weekly_matrix()
