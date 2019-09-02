#ifndef READ_FILE_H
#define READ_FILE_H

std::unordered_map<std::string, std::vector<std::string>> read_metadata(std::string);
std::vector<double> read_table_column(std::string, int);

#endif