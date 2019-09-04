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

struct whereCondition 
{
	bool num_first;
	std::string whereTable;
	std::string whereColumn;
	double whereValue;
	hsql::OperatorType oper;
};

whereCondition get_where_condition(hsql::Expr* binaryExpr) {

	if(binaryExpr->opType < hsql::kOpEquals || binaryExpr->opType > hsql::kOpGreaterEq || binaryExpr->opType == hsql::kOpNotEquals)
	{
		std::cerr << "Error: Operation not supported.\n";
		exit(1);
	}

	whereCondition cond;
	cond.num_first = false;
	cond.whereTable = "";
	cond.oper = binaryExpr->opType;

	if(binaryExpr->expr->type == hsql::kExprColumnRef)
	{
		if(binaryExpr->expr->table) cond.whereTable = binaryExpr->expr->table;
		cond.whereColumn = binaryExpr->expr->name;

		if(binaryExpr->expr2->type == hsql::kExprLiteralFloat) cond.whereValue = binaryExpr->expr2->fval;
		else if(binaryExpr->expr2->type == hsql::kExprLiteralInt) cond.whereValue = binaryExpr->expr2->ival;
		else
		{
			std::cerr << "Operation not supported.\n";
			exit(1);
		}	
	}
	else if(binaryExpr->expr2->type == hsql::kExprColumnRef)
	{
		cond.num_first = true;
		if(binaryExpr->expr2->table) cond.whereTable = binaryExpr->expr2->table;
		cond.whereColumn = binaryExpr->expr2->name;

		if(binaryExpr->expr->type == hsql::kExprLiteralFloat) cond.whereValue = binaryExpr->expr->fval;
		else if(binaryExpr->expr->type == hsql::kExprLiteralInt) cond.whereValue = binaryExpr->expr->ival;
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

	return cond;
}

std::vector<std::vector<double>> where(hsql::Expr* whereClause, std::vector<std::vector<double>> joined_tables, 
									   std::vector<std::string>& final_tables, std::vector<std::string>& final_columns) {
	
	if(whereClause)
	{
		if(whereClause->opType != hsql::kOpAnd && whereClause->opType != hsql::kOpOr)
		{
			bool column_exists = false;
			std::vector<std::vector<double>> result_table;

			if(whereClause->opType == hsql::kOpEquals && whereClause->expr->type == hsql::kExprColumnRef && whereClause->expr2->type == hsql::kExprColumnRef)
			{ 
				for(int i = 0; i < (int) joined_tables.size(); ++i)
				{
					int flag = 0;
					double value = 0;
					int columns_exists = 0;
					for(int j = 0; j < (int) joined_tables[i].size(); ++j)
					{
						if(whereClause->expr->table == nullptr || whereClause->expr->table == final_tables[j])
						{
							if(whereClause->expr->name == final_columns[j])
							{
								columns_exists++;
								if(flag == 0)
								{
									value = joined_tables[i][j];
									flag = 1;
								}
								else if(joined_tables[i][j] != value) flag = -1;
							}
						}
						if(whereClause->expr2->table == nullptr || whereClause->expr2->table == final_tables[j])
						{
							if(whereClause->expr2->name == final_columns[j])
							{
								columns_exists++;
								if(flag == 0)
								{
									value = joined_tables[i][j];
									flag = 1;
								}
								else if(joined_tables[i][j] != value) flag = -1;
							}
						}
					}
					if(columns_exists < 2)
					{
						std::cerr << "Error: Column(s) does not exist.\n";
						exit(1);
					}
					if(flag != -1) result_table.push_back(joined_tables[i]);
				}
				return result_table;
			}

			whereCondition cond = get_where_condition(whereClause);

			for(int i = 0; i < (int) joined_tables.size(); ++i)
			{
				bool valid = true;
				for(int j = 0; j < (int) joined_tables[i].size(); ++j)
				{
					if(cond.whereTable == final_tables[j] || cond.whereTable == "")
					{
						if(cond.whereColumn == final_columns[j])
						{
							column_exists = true;
							if(cond.oper == hsql::kOpEquals && joined_tables[i][j] != cond.whereValue) valid = false;
							else if(cond.oper == hsql::kOpLess || cond.oper == hsql::kOpLessEq)
							{
								if(cond.num_first && joined_tables[i][j] < cond.whereValue) valid = false;
								else if(!cond.num_first && joined_tables[i][j] > cond.whereValue) valid = false;

								if(cond.oper == hsql::kOpLess && joined_tables[i][j] == cond.whereValue) valid = false;
							}
							else if(cond.oper == hsql::kOpGreater || cond.oper == hsql::kOpGreaterEq)
							{
								if(cond.num_first && joined_tables[i][j] > cond.whereValue) valid = false;
								else if(!cond.num_first && joined_tables[i][j] < cond.whereValue) valid = false;

								if(cond.oper == hsql::kOpGreater && joined_tables[i][j] == cond.whereValue) valid = false;
							}
						}
					}
				}
				if(!column_exists)
				{
					std::cerr << "Error: " << cond.whereTable;
					if(cond.whereTable != "") std::cerr << ".";
					std::cerr << cond.whereColumn << " does not exist.\n";
					exit(1);
				}
				if(valid) result_table.push_back(joined_tables[i]);
			}
			
			return result_table;
		}
		else
		{
			whereCondition cond1 = get_where_condition(whereClause->expr);
			whereCondition cond2 = get_where_condition(whereClause->expr2);
			
			std::vector<std::vector<double>> result_table;

			for(int i = 0; i < (int) joined_tables.size(); ++i)
			{
				int columns_exists = 0;
				int conds_fulfilled = 2;
				for(int j = 0; j < (int) joined_tables[i].size(); ++j)
				{
					if(cond1.whereTable == final_tables[j] || cond1.whereTable == "")
					{
						if(cond1.whereColumn == final_columns[j])
						{
							columns_exists++;
							if(cond1.oper == hsql::kOpEquals && joined_tables[i][j] != cond1.whereValue) conds_fulfilled--;
							else if(cond1.oper == hsql::kOpLess || cond1.oper == hsql::kOpLessEq)
							{
								if(cond1.num_first && joined_tables[i][j] < cond1.whereValue) conds_fulfilled--;
								else if(!cond1.num_first && joined_tables[i][j] > cond1.whereValue) conds_fulfilled--;

								if(cond1.oper == hsql::kOpLess && joined_tables[i][j] == cond1.whereValue) conds_fulfilled--;
							}
							else if(cond1.oper == hsql::kOpGreater || cond1.oper == hsql::kOpGreaterEq)
							{
								if(cond1.num_first && joined_tables[i][j] > cond1.whereValue) conds_fulfilled--;
								else if(!cond1.num_first && joined_tables[i][j] < cond1.whereValue) conds_fulfilled--;

								if(cond1.oper == hsql::kOpGreater && joined_tables[i][j] == cond1.whereValue) conds_fulfilled--;
							}
						}
					}
					if(cond2.whereTable == final_tables[j] || cond2.whereTable == "")
					{
						if(cond2.whereColumn == final_columns[j])
						{
							columns_exists++;
							if(cond2.oper == hsql::kOpEquals && joined_tables[i][j] != cond2.whereValue) conds_fulfilled--;
							else if(cond2.oper == hsql::kOpLess || cond2.oper == hsql::kOpLessEq)
							{
								if(cond2.num_first && joined_tables[i][j] < cond2.whereValue) conds_fulfilled--;
								else if(!cond2.num_first && joined_tables[i][j] > cond2.whereValue) conds_fulfilled--;

								if(cond2.oper == hsql::kOpLess && joined_tables[i][j] == cond2.whereValue) conds_fulfilled--;
							}
							else if(cond2.oper == hsql::kOpGreater || cond2.oper == hsql::kOpGreaterEq)
							{
								if(cond2.num_first && joined_tables[i][j] > cond2.whereValue) conds_fulfilled--;
								else if(!cond2.num_first && joined_tables[i][j] < cond2.whereValue) conds_fulfilled--;

								if(cond2.oper == hsql::kOpGreater && joined_tables[i][j] == cond2.whereValue) conds_fulfilled--;
							}
						}
					}
				}
				if(columns_exists < 2)
				{
					std::cerr << "Error: Column(s) does not exist.\n";
					exit(1);
				}
				if(whereClause->opType == hsql::kOpAnd && conds_fulfilled == 2) result_table.push_back(joined_tables[i]);
				if(whereClause->opType == hsql::kOpOr && conds_fulfilled >= 1) result_table.push_back(joined_tables[i]);
			}

			return result_table;
		}
	}

	return joined_tables;
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
	
	// Join columns in where condition
	std::string table1 = "";
	std::string table2 = "";
	std::string column1 = "";
	std::string column2 = "";
	int non_display_beg = (int) columns.size();
	if(whereClause)
	{
		if(whereClause->opType != hsql::kOpAnd && whereClause->opType != hsql::kOpOr)
		{
			if(whereClause->opType == hsql::kOpEquals && whereClause->expr->type == hsql::kExprColumnRef && whereClause->expr2->type == hsql::kExprColumnRef)
			{
				if(whereClause->expr->table) table1 = whereClause->expr->table;
				if(whereClause->expr2->table) table2 = whereClause->expr2->table;
				column1 = whereClause->expr->name;
				column2 = whereClause->expr2->name;
				columns.push_back({{table1, column1}, None});
				columns.push_back({{table2, column2}, None});
			}
			else
			{
				whereCondition cond = get_where_condition(whereClause);
				columns.push_back({{cond.whereTable, cond.whereColumn}, None});
			}
		}
		else
		{
			whereCondition cond1 = get_where_condition(whereClause->expr);
			whereCondition cond2 = get_where_condition(whereClause->expr2);
			columns.push_back({{cond1.whereTable, cond1.whereColumn}, None});
			columns.push_back({{cond2.whereTable, cond2.whereColumn}, None});
		}
	}

	std::vector<std::string> final_tables;
	std::vector<std::string> final_columns;
	std::vector<enum aggregate_functions> final_aggrs;
	std::vector<std::vector<double>> final_values;

	bool flag = false;

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
						if(i >= non_display_beg && !flag)
						{
							flag = true;
							non_display_beg = (int) final_columns.size();
						}
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

	if(!flag) non_display_beg = (int) final_columns.size();

	std::vector<std::vector<double>> filtered_tables = where(whereClause, cartesian_product(final_tables, final_values), final_tables, final_columns);

	bool first_displayed = false;
	std::vector<bool> display_column(final_tables.size(), true);
	for(int i = 0; i < non_display_beg; ++i) 
	{
		if(final_tables[i] == "" || final_tables[i] == table1 || final_tables[i] == table2 || table1 == "" || table2 == "")
		{
			if(final_columns[i] == column1 || final_columns[i] == column2) 
			{
				if(first_displayed) display_column[i] = false; 
				first_displayed = true;
			}
		}
		if(i > 0)
		{
			if(final_aggrs[i] == None || final_aggrs[i-1] == None)
			{
				if(final_aggrs[i] != final_aggrs[i-1])
				{
					std::cerr << "Error: Aggregation function not applied across all tables\n";
					exit(1);
				}
			}
		}
	}

	for(int i = 0; i < non_display_beg; ++i) 
	{
		if(display_column[i]) 
		{
			if(i != 0) std::cout << ',';
			if(final_aggrs[i] == Sum) std::cout << "SUM(";
			else if(final_aggrs[i] == Average) std::cout << "AVERAGE(";
			else if(final_aggrs[i] == Max) std::cout << "MAX(";
			else if(final_aggrs[i] == Min) std::cout << "MIN(";

			std::cout << final_tables[i] << '.' << final_columns[i];

			if(final_aggrs[i] != None) std::cout << ')';
		}
	}
	std::cout << std::endl;
	
	for(int i = 1; i < (int) filtered_tables.size(); ++i) 
	{
		for(int j = 0; j < non_display_beg; ++j)
		{
			if(final_aggrs[j] == Sum || final_aggrs[j] == Average) filtered_tables[0][j] += filtered_tables[i][j];
			else if(final_aggrs[j] == Max) filtered_tables[0][j] = std::max(filtered_tables[0][j], filtered_tables[i][j]);
			else if(final_aggrs[j] == Min) filtered_tables[0][j] = std::min(filtered_tables[0][j], filtered_tables[i][j]);
		}
	}
	for(int i = 0; i < non_display_beg; ++i) if(final_aggrs[i] == Average && filtered_tables.size() > 0) filtered_tables[0][i] /= filtered_tables.size();

	
	for(int i = 0; i < (int) filtered_tables.size(); ++i) 
	{
		for(int j = 0; j < non_display_beg; ++j) 
		{
			if(display_column[j])
			{
				if(j != 0) std::cout << ',';
				std::cout << filtered_tables[i][j];
			}
		}
		std::cout << std::endl;
		if((int) final_aggrs.size() > 0 && final_aggrs[0] != None) break;
	}
	return;
}