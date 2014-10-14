#include <iostream>
#include <cstdlib>
#include <string>
#include <sstream>
#include <fstream>
#include <utility>
#include <queue>

using namespace std;

int main(int argc, char** argv){
   
    char* filename; 
    char* bin_filename;
    if(argc == 3){
        filename = argv[1];
        bin_filename = argv[2];
    } else {
        cout << "Syntax: " << endl;
        cout << argv[0] << " <input text filename> <output binary filename>" << endl;
        exit(0);
    }

    ofstream output(bin_filename, ios::binary);
    ifstream input(filename);
    int count = 0;
    int item, numitem;
    int transaction_id = 0;
    int transaction_item_count = 0;
    pair<int, int> transaction_item;
    queue< pair<int, int> > transaction;
    for(string line; getline(input, line); ){
        count++;
        istringstream iss(line);

        if(count < 4){
            int n;
            iss >> n;
            output.write(reinterpret_cast <const char*> (&n), sizeof(int));
            continue;
        }

        // Read the items for a given transaction
        while(iss >> item >> numitem){
            transaction_item = make_pair(item, numitem);
            transaction.push(transaction_item);
        }
        // Write the transaction_id, number of items in the transaction to the file
        output.write(reinterpret_cast <const char*> (&transaction_id), sizeof(int));
        int transaction_size = transaction.size();
        output.write(reinterpret_cast <const char*> (&transaction_size), sizeof(int));
       
        // Write the items of transaction to the output file 
        while(!transaction.empty()){
            transaction_item = transaction.front();
            transaction.pop();
            output.write(reinterpret_cast <const char*> (&transaction_item.first), sizeof(int));
            output.write(reinterpret_cast <const char*> (&transaction_item.second), sizeof(int));
        }

        // Increment the transaction_id
        transaction_id++;

    }
    output.close();

    cout << "DONE..." << endl;
}
