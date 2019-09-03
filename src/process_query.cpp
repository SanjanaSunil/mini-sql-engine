#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>

#include "SQLParser.h"
#include "util/sqlhelper.h"
#include "read_file.h"
#include "run_query.h"

void get_fields(const hsql::SQLStatement* query, TABLE_MAP& tables_columns) {
	
	hsql::SelectStatement* sel = (hsql::SelectStatement*) query;
	
	// Get column names
	COLUMN_MAP columns;
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

			std::string col_table_name = "";
			if((*(*sel->selectList)[i]->exprList)[0]->table)
			{	
				col_table_name = (*(*sel->selectList)[i]->exprList)[0]->table;
			}
			std::string column = (*(*sel->selectList)[i]->exprList)[0]->getName();

			if(aggr_funct == "SUM") columns.push_back({{col_table_name, column}, Sum});
			else if(aggr_funct == "AVERAGE") columns.push_back({{col_table_name, column}, Average});
			else if(aggr_funct == "MAX") columns.push_back({{col_table_name, column}, Max});
			else if(aggr_funct == "MIN") columns.push_back({{col_table_name, column}, Min});
			else
			{
				fprintf(stderr, "Error: Unknown function.\n");
				exit(1);
			}
		}
		else if((*sel->selectList)[i]->type == 4)
		{
			std::string col_table_name = "";
			if((*sel->selectList)[i]->table)
			{	
				col_table_name = (*sel->selectList)[i]->table;
			}
			columns.push_back({{col_table_name, "*"}, None});
		}
		else if((*sel->selectList)[i]->type == 6)
		{
			std::string col_table_name = "";
			if((*sel->selectList)[i]->table)
			{	
				col_table_name = (*sel->selectList)[i]->table;
			}
			columns.push_back({{col_table_name, (*sel->selectList)[i]->getName()}, None});
		}
		else
		{
			fprintf(stderr, "Error: Invalid SQL query.\n");
			exit(1);
		}
	}

	// Get table names
	std::vector<std::string> tables;
	if(!sel->fromTable) 
	{
		fprintf(stderr, "Error: No tables used.\n");
		exit(1);
	}
	if(sel->fromTable->type == 0) tables.push_back(sel->fromTable->getName());
	else
	{
		for(int i = 0; i < (int) sel->fromTable->list->size(); ++i)
		{
			tables.push_back((*sel->fromTable->list)[i]->name);
		}	
	}

	select(tables_columns, tables, columns, sel->whereClause);
	// // Check distinct
	// std::cout << sel->selectDistinct << std::endl;

	return;
} 

int process_query(std::string query, TABLE_MAP tables_columns) {

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

	for(auto i = 0u; i < result.size(); ++i) 
	{
		// hsql::printStatementInfo(result.getStatement(i));
		get_fields(result.getStatement(i), tables_columns);
	}
    
    return 0;
} 