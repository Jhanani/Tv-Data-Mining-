import pandas as pd
import numpy as np

def utility_value_check():
    weeks = ["4_1","4_7","4_14","4_21","4_28","5_5","5_12","5_19","5_26","6_2","6_9","6_16","6_23","6_30"]
    df = pd.DataFrame()
    utility_value_list1 = list()
    utility_value_list2 = list()
    for week in weeks:
        weekly_file = 'output_viewtime_matrix_%s_501.csv'%week
        df = pd.read_csv(weekly_file)
        try:
            df1 = df[(df['NCIS']!=0) & (df['NCIS: Los Angeles']!=0)]
            utility_value1 = df1['NCIS'].sum() + df1['NCIS: Los Angeles'].sum()
        except KeyError:
            utility_value1 = 0
        utility_value_list1.append(utility_value1)
        try:
            df2 = df[(df['Modern Family']!=0) & (df['The Big Bang Theory']!=0)]
            utility_value2 = df2['Modern Family'].sum() + df2['The Big Bang Theory'].sum()
        except KeyError:
            utility_value2 = 0
        utility_value_list2.append(utility_value2)
    print 'utility values for NCIS and NCIS Los Angeles',
    print utility_value_list1
    print sum(utility_value_list1)
    print 'utility values for Modern Family and The Big Bang Theory',
    print utility_value_list2
    print sum(utility_value_list2)

if __name__ == '__main__':
    utility_value_check()
