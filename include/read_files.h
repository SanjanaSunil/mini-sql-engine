#ifndef READ_FILES_H
#define READ_FILES_H

#include <string>
#include <vector>
#include <unordered_map>

using namespace std;

unordered_map<string, vector<string>> read_metadata(string);
vector<double> read_table_column(string, int);

#endif