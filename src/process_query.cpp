#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>

#include "SQLParser.h"
#include "util/sqlhelper.h"
#include "read_file.h"

typedef std::unordered_map<std::string, std::vector<std::string>> TABLE_MAP;
enum aggregate_functions{None, Sum, Average, Max, Min};


void select(TABLE_MAP& tables_columns, std::vector<std::string>& tables, std::vector<std::pair<std::pair<std::string, std::string>, enum aggregate_functions>>& columns) {

	std::vector<std::string> final_tables;
	std::vector<std::string> final_columns;
	std::vector<std::vector<double>> final_values;

	for(int i = 0; i < (int) columns.size(); ++i)
	{
		for(int j = 0; j < (int) tables.size(); ++j)
		{
			if(tables_columns.find(tables[j]) == tables_columns.end())
			{
				std::cerr << "Error: Table " << tables[j] << " does not exist\n";
				exit(1);
			}

			for(int k = 0; k < (int) tables_columns[tables[j]].size(); ++k)
			{
				if(tables[j] == columns[i].first.first || columns[i].first.first == "")
				{
					if(tables_columns[tables[j]][k] == columns[i].first.second || columns[i].first.second == "*")
					{
						final_tables.push_back(tables[j]);
						final_columns.push_back(tables_columns[tables[j]][k]);

						std::vector<double> col_values = read_table_column(tables[j], k);
						if(columns[i].second == Sum || columns[i].second == Average)
						{
							double total = 0;
							for(std::vector<double>::iterator it = col_values.begin(); it != col_values.end(); ++it) total += *it;
							
							if(columns[i].second == Sum) final_values.push_back({total});
							else final_values.push_back({total / ((double) col_values.size())});
						}
						else if(columns[i].second == Max)
						{
							final_values.push_back({*max_element(col_values.begin(), col_values.end())});
						}
						else if(columns[i].second == Min)
						{
							final_values.push_back({*min_element(col_values.begin(), col_values.end())});
						}
						else
						{
							final_values.push_back(col_values);
						}		
					}
				}
			}
		}
	}

	for(int i = 0; i < (int) final_tables.size(); ++i) 
	{
		std::cout << final_tables[i] << "." << final_columns[i] << " : ";
		for(int j = 0; j < (int) final_values[i].size(); ++j) std::cout << final_values[i][j] << " ";
		std::cout << std::endl;
	}
}


void run_query(const hsql::SQLStatement* query, TABLE_MAP& tables_columns) {
	
	hsql::SelectStatement* sel = (hsql::SelectStatement*) query;
	
	// Get column names
	std::vector<std::pair<std::pair<std::string, std::string>, enum aggregate_functions>> columns;
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
	// if(sel->whereClause)
	// {
	// 	std::cout << sel->whereClause->expr->name << std::endl;
	// 	std::cout << sel->whereClause->expr2->ival << std::endl;
	// 	std::cout << sel->whereClause->opType << std::endl; 
	// }
	
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