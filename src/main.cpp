#include <iostream>
#include "parser.h"

int main(int argc, char *argv[]) {

    if(argc != 2)
    {
        std::cout << "Usage: ./a.out \"SQL query\" \n";
    }
    else
    {
        parse_query(argv[1]);
    }

    return 0;
}