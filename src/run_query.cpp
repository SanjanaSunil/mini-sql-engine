#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>

#include "SQLParser.h"
#include "run_query.h"
#include "read_file.h"
#include "helpers.h"


void where(hsql::Expr* whereClause, std::vector<std::vector<double>>& joined_tables) {

	if(whereClause)
	{
		if(whereClause->opType != hsql::kOpAnd && whereClause->opType != hsql::kOpOr)
		{
			if(whereClause->opType < hsql::kOpEquals || whereClause->opType > hsql::kOpGreaterEq || whereClause->opType == hsql::kOpNotEquals)
			{
				std::cerr << "Operation not supported.\n";
				exit(1);
			}

			std::string whereTable = "";
			std::string whereColumn;
			double whereValue;
			if(whereClause->expr->type == hsql::kExprColumnRef)
			{
				if(whereClause->expr->table) whereTable = whereClause->expr->table;
				whereColumn = whereClause->expr->name;

				if(whereClause->expr2->type == hsql::kExprLiteralFloat) whereValue = whereClause->expr2->fval;
				else if(whereClause->expr2->type == hsql::kExprLiteralInt) whereValue = whereClause->expr2->ival;
				else
				{
					std::cerr << "Operation not supported.\n";
					exit(1);
				}
			}
			else if(whereClause->expr2->type == hsql::kExprColumnRef)
			{
				if(whereClause->expr2->table) whereTable = whereClause->expr2->table;
				whereColumn = whereClause->expr2->name;

				if(whereClause->expr->type == hsql::kExprLiteralFloat) whereValue = whereClause->expr->fval;
				else if(whereClause->expr->type == hsql::kExprLiteralInt) whereValue = whereClause->expr->ival;
				else
				{
					std::cerr << "Operation not supported.\n";
					exit(1);
				}
			}
			else
			{
				std::cerr << "Operation not supported.\n";
				exit(1);
			}

			std::cout << whereTable << " " << whereColumn << " : " << whereValue << std::endl;
		}
		else
		{
			// Take care of and and or here
			std::cout << whereClause->opType << std::endl;

			std::cout << whereClause->expr->opType << std::endl;
			std::cout << whereClause->expr->expr->name << std::endl;
			std::cout << whereClause->expr->expr2->ival << std::endl;

			std::cout << whereClause->expr2->opType << std::endl;  
			std::cout << whereClause->expr2->expr->opType << std::endl;
			std::cout << whereClause->expr2->expr2->ival << std::endl;
		}
	}

	return;
}

void select(TABLE_MAP& tables_columns, std::vector<std::string>& tables, COLUMN_MAP& columns, hsql::Expr* whereClause) {

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

	std::vector<std::vector<double>> joined_tables = cartesian_product(final_tables, final_values);

	// for(int i = 0; i < (int) final_tables.size(); ++i) 
	// {
	// 	std::cout << final_aggrs[i] << " of " << final_tables[i] << "." << final_columns[i] << " : ";
	// 	for(int j = 0; j < (int) final_values[i].size(); ++j) std::cout << final_values[i][j] << " ";
	// 	std::cout << std::endl;
	// }

	// std::cout << "===========================\n";
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

	where(whereClause, joined_tables);

	return;
}