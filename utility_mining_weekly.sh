#!/bin/bash

#input_file_prefix="output_viewtime_normalized_matrix_"
#input_file_prefix="output_viewtime_matrix_"
input_file_prefix="output_viewtime_daily_matrix_"
declare -a date_arr=("4_1" "4_7" "4_14" "4_21" "4_28" "5_5" "5_12" "5_19" "5_26" "6_2" "6_9" "6_16" "6_23")
#declare -a date_arr=("6_2" "6_9" "6_16" "6_23")
#declare -a date_arr=("4_1")

function utility_mining {
    #echo $1$2$3
    cd ./utility_mining
    python generate_utility_mine_data_digitize.py "../$1$2$3"
    ./convert_data2bin utility_mine_transation_file.txt utility_mine_transation_file.bin
    ./convert_price2bin utility_mine_cost_file.txt utility_mine_cost_file.bin
    ./tran_utility/utility_mine utility_mine_transation_file.bin utility_mine_cost_file.bin 0.01
    cd ..
    python utility_itemset_analysis.py $1$2$3
    python parse_results.py "utility_results_$1$2$3"
}


for date in "${date_arr[@]}"
do
    for postfix in "_618_income_added.csv" "_501_income_added.csv"
    do
        utility_mining $input_file_prefix $date $postfix
    done
done
