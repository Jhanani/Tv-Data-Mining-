
import sys

def main(input_filename):

    # Output filename
    temp = input_filename.strip('\t\r\n ').split('.')
    output_filename = temp[0] + '_parsed.' + temp[1]
    
    fp = open(input_filename)
    fp_out = open(output_filename, 'w')

    # Read the header
    header = None
    for line in fp:
        header = line.strip('\t\r\n\ ').split(',')
        break
    #print header
   
    new_header = list()
    new_header.extend(header[:2])
    new_header.append('Program Itemset')
    new_header.append('Rule_from: Program 1')
    new_header.append('Rule_from: Program 1 Network')
    new_header.append('Rule_from: Program 1 Day')
    new_header.append('Rule_from: Program 1 Slot')
    new_header.append('Rule_from: Program 2')
    new_header.append('Rule_from: Program 2 Network')
    new_header.append('Rule_from: Program 2 Day')
    new_header.append('Rule_from: Program 2 Slot')
    new_header.append('Rule_to: Program')
    new_header.append('Rule_to: Program Network')
    new_header.append('Rule_to: Program Day')
    new_header.append('Rule_to: Program Slot')
    new_header.extend(header[5:])
    #print new_header
    fp_out.write(','.join(new_header) + '\n')

    # Read the contents of the file
    for line in fp:
        temp = list()
        entity = line.strip('\t\r\n\ ').split(',')
        # Get the results till itemset
        temp.extend(entity[:3])

        # Rule From
        rule_from = entity[3].strip('\t\r\n\ ')
        num_items = len(rule_from.split(';'))
        if num_items == 2:
            # do something
            items = rule_from.split(';')
            # First Item in the rule
            rule_from_item_1 = items[0].split('_')
            rule_from_item_1_program = str(rule_from_item_1[0])
            rule_from_item_1_network = str(rule_from_item_1[1])
            rule_from_item_1_day = str(rule_from_item_1[2])
            rule_from_item_1_slot = str(rule_from_item_1[3])
            temp.extend([rule_from_item_1_program, rule_from_item_1_network, rule_from_item_1_day, rule_from_item_1_slot])
            # Second Item in the rule 
            rule_from_item_2 = items[1].split('_')
            rule_from_item_2_program = str(rule_from_item_2[0])
            rule_from_item_2_network = str(rule_from_item_2[1])
            rule_from_item_2_day = str(rule_from_item_2[2])
            rule_from_item_2_slot = str(rule_from_item_2[3])
            temp.extend([rule_from_item_2_program, rule_from_item_2_network, rule_from_item_2_day, rule_from_item_2_slot])
            #print temp
            #break
        else:
            rule_from = rule_from.split('_')
            rule_from_program = str(rule_from[0])
            rule_from_network = str(rule_from[1])
            rule_from_day = str(rule_from[2])
            rule_from_slot = str(rule_from[3])
            temp.extend([rule_from_program, rule_from_network, rule_from_day, rule_from_slot, '', '', '', ''])

        # Rule To
        rule_to = entity[4].strip('\t\r\n ').split('_')
        rule_to_program = str(rule_to[0])
        rule_to_network = str(rule_to[1])
        rule_to_day = str(rule_to[2])
        rule_to_slot = str(rule_to[3])
        temp.extend([rule_to_program, rule_to_network, rule_to_day, rule_to_slot])

        # Conf, Lift
        temp.extend(entity[5:])
        fp_out.write(','.join(temp) + '\n')
        #print temp
        #print entity
        #break
    fp.close()
    fp_out.close()

if __name__ == '__main__':
    input_filename = None

    try:
        input_filename = sys.argv[1]
    except:
        print 'Syntax Error'
        print sys.aryv[0], '<input csv file>'
        exit(0)

    main(input_filename)
