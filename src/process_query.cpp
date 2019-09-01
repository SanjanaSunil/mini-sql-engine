#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>

#include "SQLParser.h"
#include "util/sqlhelper.h"

typedef std::unordered_map<std::string, std::vector<std::string>> TABLE_MAP;

void run_query(const hsql::SQLStatement* query) {
	
	hsql::SelectStatement* sel = (hsql::SelectStatement*) query;

	// // Check distinct
	// std::cout << sel->selectDistinct << std::endl;

	// // Table names
	// std::cout << sel->fromTable->list->size() << std::endl;
	// for(int i = 0; i < sel->fromTable->list->size(); ++i)
	// {
	// 	std::cout << (*sel->fromTable->list)[i]->name << std::endl;
	// }

	// Get Table names and aggregate functions
	// std::cout << sel->selectList->size() << std::endl;
	// for(int j=0; j<sel->selectList->size(); ++j) 
	// {
	// 	printf("Sanjana: %s\n", (*sel->selectList)[j]->getName());
	// 	if((*sel->selectList)[j]->type == 7)
	// 		printf("Sanjana: %s\n", (*(*sel->selectList)[j]->exprList)[0]->getName());
	// }

	return;
} 

int process_query(std::string query, TABLE_MAP tables) {

    hsql::SQLParserResult result;
    hsql::SQLParser::parse(query, &result);

    if(!result.isValid())
    {
        fprintf(stderr, "Given string is not a valid SQL query.\n");
        fprintf(stderr, "%s (L%d:%d)\n",
                result.errorMsg(),
                result.errorLine(),
                result.errorColumn());

		return 1;
    }

	// printf("Number of statements: %lu\n\n", result.size());

	for(auto i = 0u; i < result.size(); ++i) 
	{
		run_query(result.getStatement(i));
		// hsql::printStatementInfo(result.getStatement(i));
	}
    
    return 0;
} 