import sys
import pandas as pd
import MySQLdb
import fwm_config as fwm_config

def get_income(dma, dev):
    db = fwm_config.Database()
    db_ref_table = 'fwm_ref_04_2014'
    db_demo_table = 'fwm_demo_04_2014'
    query_household = """
                    SELECT device_id, household_id FROM {0}
                    WHERE device_id IN %s AND dma_code = %s;
                    """.format(db_ref_table)
    household_id = db.query_tuple(query_household,(dev,dma))
    household_dict = dict()
    for entry in household_id:
        household_dict.setdefault(entry['device_id'],set()).add(entry['household_id'])
        #if entry['device_id'] in household_dict and entry['household_id']!=household_dict[entry['device_id']]:
        #    print 'multiple device ids within one dma'
        #    print entry
        #    print household_dict[entry['device_id']]
        #else:
        #    household_dict[entry['device_id']] = entry['household_id']
    query_income = """
                SELECT household_id, hh_income FROM {0} WHERE household_id IN %s;
                """.format(db_demo_table)
    #print 'dev length:', len(set(dev))
    #print 'dev length:', len(household_dict.keys())
    #print 'household id number:',len(set(household_dict.values()))
    household_id_list = list(set([item for s in household_dict.values() for item in s]))
    incomes = db.query_new(query_income, household_id_list)
    household_income_dict = dict()
    for entry in incomes:
        if entry['household_id'] in household_income_dict:
            print 'same household id with different income'
            print entry
        else:
            household_income_dict[entry['household_id']] = entry['hh_income']
    #print 'household id number:',len(household_income_dict)
    dev_income_dict = dict()
    #count = 0
    for device in dev:
        try:
            household_list = list(household_dict[device])
            num = 0
            income_sum = 0
            for house in household_list:
                num += 1
                if house not in household_income_dict or household_income_dict[house] is None:
                    num -= 1
                else:
                    income_sum += income_level_to_value(household_income_dict[house])
            #dev_income_dict[device] = household_income_dict[household_dict[device]]
            if num == 0:
                dev_income_dict[device] = None
            else:
                dev_income_dict[device] = income_sum*1.0/num
        except KeyError:
            dev_income_dict[device] = None
            #count += 1
            #print 'key error'
            #print dev
            #print household_dict[dev]
    income_list = dev_income_dict.values()
    income_list = [income for income in income_list if income is not None]
    #income_list = map(income_level_to_value,income_list)
    average = sum(income_list)*1.0/len(income_list)
    #print income_list
    #print average
    for dev in dev_income_dict:
        if dev_income_dict[dev] is None:
            dev_income_dict[dev] = -1
        #else:
        #    dev_income_dict[dev] = income_level_to_value(dev_income_dict[dev])
    return dev_income_dict

def income_level_to_value(level):
    if level == '1':
        return 1.0
    elif level == '2':
        return 1.75
    elif level == '3':
        return 2.5
    elif level == '4':
        return 3.5
    elif level == '5':
        return 4.5
    elif level == '6':
        return 5.5
    elif level == '7':
        return 6.5
    elif level == '8':
        return 7.5
    elif level == '9':
        return 8.5
    elif level == 'A':
        return 9.5
    elif level == 'B':
        return 11.25
    elif level == 'C':
        return 13.75
    elif level == 'D':
        return 16
    elif level is None:
        return None
    else:
        print level
        return level

def multiply_income(row, *args):
    income_dict = args[0]
    #print row
    #print income_dict[row.name]
    newRow = pd.Series(data=map(lambda x:x*income_dict[row.name],row.values),index=row.index,name=row.name)
    #print newRow
    return newRow

def create_viewtime_income_matrix():
    file_prefix = 'output_viewtime_daily_matrix_'
    date_arr = ['4_1','4_7','4_14','4_21','4_28','5_5','5_12','5_19','5_26','6_2','6_9','6_16','6_23']
    for dma in [618]:
        for date in date_arr:
            viewtime_file = file_prefix + date + '_%s.csv'%dma
            print 'start analyzing file ',viewtime_file
            df = pd.read_csv(viewtime_file, index_col=0)
            dev_list = df.index.tolist()
            #print dev_list
            income_dict = get_income(dma, dev_list)
            #print income_dict
            #print df.head()
            df = df.apply(multiply_income, axis=1, args=(income_dict,))
            #print df.head()
            df.to_csv(file_prefix + date + '_%s_income_added.csv'%dma)
    print 'all done.'

if __name__ == '__main__':
    create_viewtime_income_matrix()
