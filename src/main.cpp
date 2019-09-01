#include <iostream>
#include <string>

#include "parser.h"

int main(int argc, char *argv[]) {

    if(argc != 2)
    {
        std::cout << "Usage: ./a.out \"SQL query\" \n";
        return 1;
    }

    std::string query = argv[1];
    return parse_query(query);
}