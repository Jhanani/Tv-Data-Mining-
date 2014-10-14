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
    int item_id = 0;
    float item_cost;
    for(string line; getline(input, line); ){
        istringstream iss(line);

        iss >> item_id >> item_cost;
        
        // Write the transaction_id, number of items in the transaction to the file
        output.write(reinterpret_cast <const char*> (&item_cost), sizeof(float));
    }
    output.close();
    cout << "DONE..." << endl;
}
