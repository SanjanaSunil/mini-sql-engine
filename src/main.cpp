#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include "SQLParser.h"
#include "util/sqlhelper.h"
#include "read_files.h"
#include "process_from.h"

using namespace std;

int main(int argc, char* argv[]) {

    if(argc != 2)
    {
        cout << "Usage: ./a.out \"SQL query\" \n";
        return 1;
    }

    unordered_map<string, vector<string>> tables_columns = read_metadata("files/metadata.txt");

    string queries = argv[1];
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(queries, &result);

    if(!result.isValid())
    {
        fprintf(stderr, "Given string is not a valid SQL query.\n");
        fprintf(stderr, "%s (L%d:%d)\n", result.errorMsg(), result.errorLine(), result.errorColumn());
        exit(1);
    }

    for(auto i = 0u; i < result.size(); ++i) 
    {
        // hsql::printStatementInfo(result.getStatement(i));
        process_from(result.getStatement(i), tables_columns);
    }

    return 0;
}