#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include "read_file.h"
#include "process_query.h"

int main(int argc, char* argv[]) {

    if(argc != 2)
    {
        std::cout << "Usage: ./a.out \"SQL query\" \n";
        return 1;
    }

    std::string query = argv[1];
    return process_query(query, read_metadata("files/metadata.txt"));
}