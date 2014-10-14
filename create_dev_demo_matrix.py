import sys
import pandas as pd
import fwm_config as fwm_config
from create_viewtime_income_matrix import get_income
#from create_viewtime_income_matrix import income_level_to_value

def home_market_to_value(level):
    if level == 'A':
        return 13
    elif level >= 'B' and level <= 'L':
        return 37.5 + 25*(ord(level) - ord('B'))
    elif level >= 'M' and level <= 'P':
        return 325 + 50*(ord(level) - ord('M'))
    elif level >= 'Q' and level <= 'S':
        return 625 + 250*(ord(level) - ord('Q'))
    elif level is None:
        return None
    else:
        print level
        return level

def net_worth_to_value(level):
    if level == 1:
        return 1
    elif level >= 2 and level <= 3:
        return 2500 + 5000*(level-2)
    elif level == 4:
        return 17500
    elif level == 5:
        return 37500
    elif level == 6:
        return 75000
    elif level == 7:
        return 175000
    elif level >= 8 and level <= 9:
        return 375000 + 250000*(level-8)
    elif level is None:
        return None
    else:
        print level
        return level

def demo_level_to_value(level, demo_name):
    if demo_name == 'hh_net_worth':
        return net_worth_to_value(level)
    elif demo_name == 'home_market_value':
        return home_market_to_value(level)

def get_demo_value(dma, dev, demo_name):
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
    if demo_name == 'hh_net_worth':
        query_demo = """
                    SELECT household_id, hh_net_worth FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    elif demo_name == 'home_market_value':
        query_demo = """
                    SELECT household_id, home_market_value FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    household_id_list = list(set([item for s in household_dict.values() for item in s]))
    demo_data = db.query_new(query_demo, household_id_list)
    household_demo_dict = dict()
    for entry in demo_data:
        if entry['household_id'] in household_demo_dict:
            print 'same household id with different demo info.'
            print entry
        else:
            household_demo_dict[entry['household_id']] = entry[demo_name]
    dev_value_dict = dict()
    for device in dev:
        try:
            household_list = list(household_dict[device])
            num = 0
            value_sum = 0
            for house in household_list:
                num += 1
                if house not in household_demo_dict or household_demo_dict[house] is None:
                    num -= 1
                else:
                    value_sum += demo_level_to_value(household_demo_dict[house],demo_name)
            if num == 0:
                dev_value_dict[device] = None
            else:
                dev_value_dict[device] = value_sum*1.0/num
        except KeyError:
            dev_value_dict[device] = None
    value_list = dev_value_dict.values()
    value_list = [value for value in value_list if value is not None]
    average = sum(value_list)*1.0/len(value_list)
    for dev in dev_value_dict:
        if dev_value_dict[dev] is None:
            dev_value_dict[dev] = -1
    return dev_value_dict

def get_demo(dma, dev, demo_name):
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
    household_id_list = list(set([item for s in household_dict.values() for item in s]))
    if demo_name == 'hh_head_marital_status':
        query_demo = """
                    SELECT household_id, hh_head_marital_status FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    elif demo_name == 'children_present':
        query_demo = """
                    SELECT household_id, children_present FROM {0} WHERE household_id IN %s;
                    """.format(db_demo_table)
    demo_data = db.query_new(query_demo, household_id_list)
    household_demo_dict = dict()
    for entry in demo_data:
        if entry['household_id'] in household_demo_dict:
            print 'same household_id with different demo'
            print entry
        else:
            household_demo_dict[entry['household_id']] = entry[demo_name]
    dev_demo_dict = dict()
    for device in dev:
        try:
            household_list = list(household_dict[device])
            demo_list = []
            for house in household_list:
                try:
                    demo_list.append(household_demo_dict[house])
                except KeyError:
                    pass
            demo_list = [item for item in demo_list if item is not None]
            if len(set(demo_list)) != 1:
                #print 'same device id with diff demo:',set(demo_list)
                dev_demo_dict[device] = None
            else:
                dev_demo_dict[device] = demo_list[0]
        except KeyError:
            dev_demo_dict[device] = None
    #print dev_demo_dict
    return dev_demo_dict

def create_dev_demo_matrix(dma, filename):
    df = pd.read_csv(filename, index_col=0)
    #print df.head()
    #print df.shape
    dev_list = df.index.tolist()
    income_dict = get_income(dma, dev_list)
    marital_dict = get_demo(dma, dev_list, 'hh_head_marital_status')
    child_dict = get_demo(dma, dev_list, 'children_present')
    home_market_value_dict = get_demo_value(dma, dev_list, 'home_market_value')
    net_worth_dict = get_demo_value(dma, dev_list, 'hh_net_worth')
    #new_df = pd.DataFrame(income_dict.items(), columns=['Device_id','Income'])
    new_df = pd.DataFrame(dev_list, columns=['Device_id'])
    #print new_df.shape
    new_df['Income'] = map(lambda x:income_dict[x], new_df['Device_id'])
    new_df['Marital'] = map(lambda x:marital_dict[x], new_df['Device_id'])
    new_df['Child'] = map(lambda x:child_dict[x], new_df['Device_id'])
    new_df['HomeMarketValue'] = map(lambda x:home_market_value_dict[x], new_df['Device_id'])
    new_df['NetWorth'] = map(lambda x:net_worth_dict[x], new_df['Device_id'])
    #print new_df['Marital'].count()
    #print new_df['Child'].count()
    #print new_df.head()
    return new_df

def create_dev_demo_matrices():
    file_path = '/files2/Temp/'
    file_prefix = 'output_viewtime_matrix_'
    date_arr = ['4_1','4_7','4_14','4_21','4_28','5_5','5_12','5_19','5_26','6_2','6_9','6_16','6_23']
    for dma in [501,618]:
        for date in date_arr:
            input_file = file_path+'csv_%s/'%dma+file_prefix+date+'_%s.csv'%dma
            output_file = file_prefix+date+'_%s_demo_info.csv'%dma
            print 'start analyzing file ',input_file
            df = create_dev_demo_matrix(dma, input_file)
            df.to_csv(output_file, index=False)
    print 'all done.'

if __name__=='__main__':
    create_dev_demo_matrices()
    #file = '/files2/Temp/csv_618/output_viewtime_matrix_4_1_618.csv'
    #create_dev_demo_matrix(618, file)
