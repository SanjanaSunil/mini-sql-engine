#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>

typedef std::unordered_map<std::string, std::vector<std::string>> TABLE_MAP;

TABLE_MAP read_metadata(std::string metadata_file_path) {

    std::string line;
    std::ifstream metadata_file (metadata_file_path);
    TABLE_MAP tables;

    int new_file = 0;
    std::string cur_table = "";

    if(!metadata_file.is_open())
    {
        printf("Unable to open metadata file\n");
        exit(1);
    }

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
            if(new_file == 1) cur_table = line;
            else tables[cur_table].push_back(line);
            new_file = 0;
        }
    }

    metadata_file.close();
    
    return tables;
}