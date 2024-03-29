#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <unordered_map>

using namespace std;

// Mapping of tables to the columns it contains
unordered_map<string, vector<string>> read_metadata(string metadata_file_path) {

    string line;
    ifstream metadata_file (metadata_file_path);

    if(!metadata_file.is_open())
    {
        fprintf(stderr, "Unable to open metadata file\n");
        exit(1);
    }

    int new_file = 0;
    string cur_table = "";
    unordered_map<string, vector<string>> tables;

    while(metadata_file >> line)
    {
        if(line.compare("<begin_table>") == 0) new_file = 1;
        else if(line.compare("<end_table>") == 0)
        {
            new_file = 0;
            cur_table = "";
        } 
        else
        {
            if(new_file == 1) 
            {
                cur_table = line;
                tables[cur_table] = {};
            }
            else tables[cur_table].push_back(line);
            new_file = 0;
        }
    }

    metadata_file.close();

    return tables;
}

// Read a specific column in a table
vector<double> read_table_column(string table_name, int column_no) {

    ifstream fin ("files/" + table_name + ".csv");

    if(!fin.is_open())
    {
        fprintf(stderr, "Unable to open table\n");
        exit(1);
    }

    vector<double> column_values;
    string line, word;

    while(fin >> line)
    {
        stringstream s(line);
        int col_no = 0;
        while(getline(s, word, ',')) 
        {
            if(col_no == column_no) column_values.push_back(stoi(word));
            col_no++;
        }
    }

    fin.close();

    return column_values;
}