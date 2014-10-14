import sys
import pandas as pd
import numpy as np
import math

#INPUT_FILE = '/home/fan/intern/process_db/analysis/output_viewtime_matrix_full_501.csv'
OUTPUT_FILE = './utility_mine_transation_file.txt'
OUTPUT_COST_FILE = './utility_mine_cost_file.txt'
OUTPUT_PROGRAM_FILE = './utility_mine_program_file.txt'

def main():
    print 'This is main program'
    #input_file = INPUT_FILE
    input_file = sys.argv[1]
    print input_file

    fp = open(input_file)
    programs = None
    for line in fp:
        #print line
        #programs = line.strip('\n\t\r \"').split('","')
        programs = line.strip('\n\t\r \"').split(',')
        break
    fp.close()
	
    programs = programs[1:]
    #programs[0] = ','.join(programs[0].split(',"')[1:])
	#print programs
	#print programs[:10]

    data = pd.read_csv(input_file, sep=',')
    data_np = np.array(data)[:,1:]
    num_transactions = data_np.shape[0]
    num_items = data_np.shape[1]

    print 'Original Num Transactions: ', num_transactions
    print 'Original Num Items: ', num_items

    output_file = OUTPUT_FILE
    output_cost_file = OUTPUT_COST_FILE
    output_program_file = OUTPUT_PROGRAM_FILE
    
    num_trans = 0
    write_trans_list = list()
    for item in data_np:
        item = item.astype(np.int)
       
        # digitize in units of 5 mins
        x1 = 0
        x2 = int(5*math.ceil(max(item)/60.0/5.0))
        # Added 5*60.0  to x2*60.0just in case
        bins = [x for x in range(x1, x2*60 + 5*60, 5*60)]
        digitized_item = np.digitize(item, bins) - 1
       
        #item_idx = np.where(item >= 60)[0]
        #item_val = item[item>=60]
        item_idx = np.where(digitized_item >= 1)[0]
        item_val = digitized_item[digitized_item>=1]

        if len(item_val):
            num_trans = num_trans + 1
            transaction_str = str(item_idx[0]) + ' ' + str(item_val[0])
            #print transaction_str
            for idx, val in zip(item_idx[1:], item_val[1:]):
                transaction_str = transaction_str + ' ' + str(idx) + ' ' + str(val)

            write_trans_list.append(transaction_str + '\n')
            #fp.write(transaction_str + '\n')
            #print transaction_str
        #break
   
    ## DEBUG
    #return

    fp = open(output_file, 'w')
    fp.write(str(num_trans) + '\n')
    fp.write(str(num_items) + '\n')
    fp.write(str(0) + '\n')
    for item in write_trans_list:
        fp.write(item)
    fp.close()

    fp = open(output_cost_file, 'w')
    for i in range(0, num_items):
        write_str = str(i) + ' ' + str(float(1))
        fp.write(write_str + '\n')
    fp.close()

    fp = open(output_program_file, 'w')
    count = 0
    for item in programs:
       	write_str = str(count) + ',' + item
        count = count+1
        fp.write(write_str + '\n')
    fp.close()


if __name__ == '__main__':
    main()
