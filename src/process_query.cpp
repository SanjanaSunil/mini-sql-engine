#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>

#include "SQLParser.h"
#include "util/sqlhelper.h"

typedef std::unordered_map<std::string, std::vector<std::string>> TABLE_MAP;
enum aggregate_functions{None, Sum, Average, Max, Min};


void run_query(const hsql::SQLStatement* query) {
	
	hsql::SelectStatement* sel = (hsql::SelectStatement*) query;
	
	// Get column names
	std::vector<std::pair<std::string, enum aggregate_functions>> columns;
	for(int i = 0; i < (int) sel->selectList->size(); ++i)
	{
		if((*sel->selectList)[i]->type == 7)
		{
			if((*sel->selectList)[i]->exprList->size() != 1 || (*(*sel->selectList)[i]->exprList)[0]->type != 6)
			{
				fprintf(stderr, "Error: Cannot apply aggregation function.\n");
				exit(1);
			}
			
			std::string aggr_funct = (*sel->selectList)[i]->getName();
			std::transform(aggr_funct.begin(), aggr_funct.end(), aggr_funct.begin(), ::toupper);
			std::string column = (*(*sel->selectList)[i]->exprList)[0]->getName();

			if(aggr_funct == "SUM") columns.push_back({column, Sum});
			else if(aggr_funct == "AVERAGE") columns.push_back({column, Average});
			else if(aggr_funct == "MAX") columns.push_back({column, Max});
			else if(aggr_funct == "MIN") columns.push_back({column, Min});
			else
			{
				fprintf(stderr, "Error: Unknown function.\n");
				exit(1);
			}
		}
		else if((*sel->selectList)[i]->type == 4)
		{
			columns.push_back({"*", None});
		}
		else if((*sel->selectList)[i]->type == 6)
		{
			columns.push_back({(*sel->selectList)[i]->getName(), None});
		}
		else
		{
			fprintf(stderr, "Error: Invalud SQL query.\n");
			exit(1);
		}
		
	}

	// Get table names
	std::vector<std::string> tables;
	if(sel->fromTable->type == 0) tables.push_back(sel->fromTable->getName());
	else
	{
		for(int i = 0; i < (int) sel->fromTable->list->size(); ++i)
		{
			tables.push_back((*sel->fromTable->list)[i]->name);
		}	
	}

	// // Check distinct
	// std::cout << sel->selectDistinct << std::endl;

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