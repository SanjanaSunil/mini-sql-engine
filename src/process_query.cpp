#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>

#include "SQLParser.h"
#include "util/sqlhelper.h"
#include "read_file.h"
#include "helpers.h"

enum aggregate_functions{None, Sum, Average, Max, Min};

typedef std::unordered_map<std::string, std::vector<std::string>> TABLE_MAP;
typedef std::vector<std::pair<std::pair<std::string, std::string>, enum aggregate_functions>> COLUMN_MAP;


void select(TABLE_MAP& tables_columns, std::vector<std::string>& tables, COLUMN_MAP& columns) {

	for(int i = 0; i < (int) tables.size(); ++i)
	{
		for(int j = 0; j < (int) tables.size(); ++j)
		{
			if(i != j && tables[i] == tables[j])
			{
				std::cerr << "Error: Not unique table: " << tables[i] << ".\n";
				exit(1);
			}
		}
	}

	std::vector<std::string> final_tables;
	std::vector<std::string> final_columns;
	std::vector<enum aggregate_functions> final_aggrs;
	std::vector<std::vector<double>> final_values;

	for(int i = 0; i < (int) columns.size(); ++i)
	{
		if(columns[i].first.first != "" && tables_columns.find(columns[i].first.first) == tables_columns.end())
		{
			std::cerr << "Error: Table " << columns[i].first.first << " does not exist.\n";
			exit(1);
		}

		int cnt = 0;
		bool column_exists = false;
		for(int j = 0; j < (int) tables.size(); ++j)
		{
			if(tables_columns.find(tables[j]) == tables_columns.end())
			{
				std::cerr << "Error: Table " << tables[j] << " does not exist.\n";
				exit(1);
			}

			for(int k = 0; k < (int) tables_columns[tables[j]].size(); ++k)
			{
				if(tables[j] == columns[i].first.first || columns[i].first.first == "")
				{
					if(tables_columns[tables[j]][k] == columns[i].first.second || columns[i].first.second == "*")
					{
						column_exists = true;
						if(columns[i].first.second != "*") cnt++;
						final_tables.push_back(tables[j]);
						final_columns.push_back(tables_columns[tables[j]][k]);
						final_aggrs.push_back(columns[i].second);
						final_values.push_back(read_table_column(tables[j], k));		
					}
				}
			}
		}
		if(cnt > 1)
		{
			std::cerr << "Error: " << columns[i].first.second << " exists in more than one table.\n";
			exit(1);
		}
		if(!column_exists)
		{
			std::cerr << "Error: " << columns[i].first.first;
			if(columns[i].first.first != "") std::cerr << ".";
			std::cerr << columns[i].first.second << " does not exist.\n";
			exit(1);
		}
	}

	for(int i = 0; i < (int) final_tables.size(); ++i) 
	{
		std::cout << final_aggrs[i] << " of " << final_tables[i] << "." << final_columns[i] << " : ";
		for(int j = 0; j < (int) final_values[i].size(); ++j) std::cout << final_values[i][j] << " ";
		std::cout << std::endl;
	}

	std::vector<std::vector<double>> joined_tables = cartesian_product(final_tables, final_values);

	std::cout << "===========================\n";
	for(int i = 0; i < (int) final_tables.size(); ++i) 
	{
		std::cout << final_aggrs[i] << " of " << final_tables[i] << "." << final_columns[i] << "\t";
	}
	std::cout << std::endl;
	for(int i = 0; i < (int) joined_tables.size(); ++i) 
	{
		for(int j = 0; j < (int) joined_tables[i].size(); ++j) std::cout << joined_tables[i][j] << " ";
		std::cout << std::endl;
	}
}

//***********************************************
void checkWhere(hsql::Expr*);
void printWhere(hsql::Expr* expr) {
	if(expr == NULL)
	{
		std::cerr << "NULL 2\n";
		return;
	}
	switch(expr->type) {
		case hsql::kExprOperator:
			checkWhere(expr);
			break;
		case hsql::kExprLiteralInt:
			std::cout << expr->ival << std::endl;
			break;
		case hsql::kExprColumnRef:
			if(expr->table) std::cout << expr->table << " ";
			std::cout << expr->name << std::endl;
			break;
		default:
			std::cerr << "UNKNOWN " << expr->type << '\n';
	}
}
void checkWhere(hsql::Expr* expr) {
	if(expr == NULL)
	{
		std::cerr << "NULL1\n";
		return;
	}
	std::cout << expr->opType << std::endl;
	printWhere(expr->expr);
	if (expr->expr2 != nullptr) {
        printWhere(expr->expr2);
    } else if (expr->exprList != nullptr) {
        for (hsql::Expr* e : *expr->exprList) printWhere(e);
    }
}
//***********************************************

void run_query(const hsql::SQLStatement* query, TABLE_MAP& tables_columns) {
	
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

	// Where clause
	// checkWhere(sel->whereClause);
	if(sel->whereClause)
	{
		// std::cout << sel->whereClause->opType << std::endl;

		// std::cout << sel->whereClause->expr->opType << std::endl;
		// std::cout << sel->whereClause->expr->expr->name << std::endl;
		// std::cout << sel->whereClause->expr->expr2->ival << std::endl;

		// std::cout << sel->whereClause->expr2->opType << std::endl;  
		// std::cout << sel->whereClause->expr2->expr->opType << std::endl;
		// std::cout << sel->whereClause->expr2->expr2->ival << std::endl;
	}
	
	//-------------

	select(tables_columns, tables, columns);
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

	// printf("Number of statements: %lu\n\n", result.size());

	for(auto i = 0u; i < result.size(); ++i) 
	{
		// hsql::printStatementInfo(result.getStatement(i));
		run_query(result.getStatement(i), tables_columns);
	}
    
    return 0;
} 