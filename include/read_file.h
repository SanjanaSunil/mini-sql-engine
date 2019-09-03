#ifndef READ_FILE_H
#define READ_FILE_H

typedef std::unordered_map<std::string, std::vector<std::string>> TABLE_MAP;

TABLE_MAP read_metadata(std::string);
std::vector<double> read_table_column(std::string, int);

#endif