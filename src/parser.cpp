#include <iostream>
#include <vector>
#include <string>

#include "SQLParser.h"
#include "util/sqlhelper.h"

int parse_query(std::string query) {

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

	printf("Parsed successfully!\n");
	printf("Number of statements: %lu\n\n", result.size());

	for(auto i = 0u; i < result.size(); ++i) 
	{
		// hsql::SelectStatement* sel = (hsql::SelectStatement*) result.getStatement(i);                                         
		// std::cout << sel->fromTable->getName() << std::endl;
		// std::cout << sel->selectDistinct << std::endl;
		// std::cout << sel->selectList->size() << std::endl;

		// for(int j=0; j<sel->selectList->size(); ++j) 
		// {
		// 	printf("Sanjana: %s\n", (*sel->selectList)[j]->name);
		// }
		hsql::printStatementInfo(result.getStatement(i));
	}
    
    return 0;
} 