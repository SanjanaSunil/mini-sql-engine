#include <iostream>
#include <vector>
#include <cstring>
#include <string>

void parse_query(char* query) {

    std::vector<std::string> tokens;

    char *token = std::strtok(query, " ,");
    while(token != NULL)
    {
        tokens.push_back(token);
        token = std::strtok(NULL, " ,");
    }

    for(int i=0; i<tokens.size(); ++i) std::cout << tokens[i] << '\n'; 

    return;
} 