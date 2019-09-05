#include <iostream>
#include <vector>
#include <string>

#include "SQLParser.h"
#include "config.h"
#include "helpers.h"

using namespace std;

struct whereCondition 
{
	bool num_first;
	string whereTable;
	string whereColumn;
	double whereValue;
	hsql::OperatorType oper;
};

whereCondition get_where_condition(hsql::Expr* binaryExpr) {

	if(binaryExpr->opType < hsql::kOpEquals || binaryExpr->opType > hsql::kOpGreaterEq || binaryExpr->opType == hsql::kOpNotEquals)
	{
		cerr << "Error: Operation not supported.\n";
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
			cerr << "Operation not supported.\n";
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
			cerr << "Operation not supported.\n";
			exit(1);
		}
	}
	else
	{
		cerr << "Operation not supported.\n";
		exit(1);
	}

	return cond;
}


void process_where(vector<column_data>& joined_columns, hsql::Expr* whereClause) {

    if(whereClause)
    {
		vector<vector<double>> valid_rows;
		vector<double> row;

        if(whereClause->opType != hsql::kOpAnd && whereClause->opType != hsql::kOpOr)
		{
			// Check for join condition
			if(whereClause->opType == hsql::kOpEquals && whereClause->expr->type == hsql::kExprColumnRef && whereClause->expr2->type == hsql::kExprColumnRef)
			{
				bool column1_exists = false;
				bool column2_exists = false;
				for(int i = 0; i < (int) joined_columns[0].values.size(); ++i)
				{
					int flag = 0;
					double value = 0;
					int column1_cnt = 0;
					int column2_cnt = 0;
					row.clear();
					for(int j = 0; j < (int) joined_columns.size(); ++j)
					{
						row.push_back(joined_columns[j].values[i]);
						if(whereClause->expr->table == nullptr || whereClause->expr->table == joined_columns[j].table)
						{
							if(whereClause->expr->name == joined_columns[j].column)
							{
								if(whereClause->expr->table == nullptr) column1_cnt++;
								column1_exists = true;
								if(flag == 0)
								{
									value = joined_columns[j].values[i];
									flag = 1;
								}
								else if(joined_columns[j].values[i] != value) flag = -1;
							}
						}
						if(whereClause->expr2->table == nullptr || whereClause->expr2->table == joined_columns[j].table)
						{
							if(whereClause->expr2->name == joined_columns[j].column)
							{
								if(whereClause->expr2->table == nullptr) column2_cnt++;
								column2_exists = true;
								if(flag == 0)
								{
									value = joined_columns[j].values[i];
									flag = 1;
								}
								else if(joined_columns[j].values[i] != value) flag = -1;
							}
						}
					}
					if(!column1_exists)
					{
						cerr << "Error: ";
						if(whereClause->expr->table) cerr << whereClause->expr->table << ".";
						cerr << whereClause->expr->name << " does not exist.\n";
						exit(1);
					}
					if(!column2_exists)
					{
						cerr << "Error: ";
						if(whereClause->expr2->table) cerr << whereClause->expr2->table << ".";
						cerr << whereClause->expr2->name << " does not exist.\n";
						exit(1);
					}
					if(column1_cnt > 1)
					{
						cerr << "Error: " << whereClause->expr->name << " exists in more than one table.\n";
						exit(1);
					}
					if(column2_cnt > 1)
					{
						cerr << "Error: " << whereClause->expr2->name << " exists in more than one table.\n";
						exit(1);
					}

					if(flag != -1) valid_rows.push_back(row);
				}

				replace_columns(joined_columns, valid_rows);
				return; 
			}

			bool column_exists = false;
            whereCondition cond = get_where_condition(whereClause);

			for(int i = 0; i < (int) joined_columns[0].values.size(); ++i)
			{
				int column_cnt = 0;
				bool valid = true;
				row.clear();
				for(int j = 0; j < (int) joined_columns.size(); ++j)
				{
					row.push_back(joined_columns[j].values[i]);
					if(cond.whereTable == "" || cond.whereTable == joined_columns[j].table)
					{
						if(cond.whereColumn == joined_columns[j].column)
						{
							if(cond.whereTable == "") column_cnt++;
							column_exists = true;
							if(cond.oper == hsql::kOpEquals && joined_columns[j].values[i] != cond.whereValue) valid = false;
							else if(cond.oper == hsql::kOpLess || cond.oper == hsql::kOpLessEq)
							{
								if(cond.num_first && joined_columns[j].values[i] < cond.whereValue) valid = false;
								else if(!cond.num_first && joined_columns[j].values[i] > cond.whereValue) valid = false;

								if(cond.oper == hsql::kOpLess && joined_columns[j].values[i] == cond.whereValue) valid = false;
							}
							else if(cond.oper == hsql::kOpGreater || cond.oper == hsql::kOpGreaterEq)
							{
								if(cond.num_first && joined_columns[j].values[i] > cond.whereValue) valid = false;
								else if(!cond.num_first && joined_columns[j].values[i] < cond.whereValue) valid = false;

								if(cond.oper == hsql::kOpGreater && joined_columns[j].values[i] == cond.whereValue) valid = false;
							}
						}
					}
				}
				if(!column_exists)
				{
					cerr << "Error: " << cond.whereTable;
					if(cond.whereTable != "") cerr << ".";
					cerr << cond.whereColumn << " does not exist.\n";
					exit(1);
				}
				if(column_cnt > 1)
				{
					cerr << "Error: " << cond.whereColumn << " exists in more than one table.\n";
					exit(1);
				}
				if(valid) valid_rows.push_back(row);
			}

			replace_columns(joined_columns, valid_rows);
        }
        else
        {
			bool column1_exists = false;
			bool column2_exists = false;

			whereCondition cond1 = get_where_condition(whereClause->expr);
			whereCondition cond2 = get_where_condition(whereClause->expr2);

			for(int i = 0; i < (int) joined_columns[0].values.size(); ++i)
			{
				row.clear();
				int conds_fulfilled = 2;
				int column1_cnt = 0;
				int column2_cnt = 0;
				for(int j = 0; j < (int) joined_columns.size(); ++j)
				{
					row.push_back(joined_columns[j].values[i]);
					if(cond1.whereTable == "" || cond1.whereTable == joined_columns[j].table)
					{
						if(cond1.whereColumn == joined_columns[j].column)
						{
							if(cond1.whereTable == "") column1_cnt++;
							column1_exists = true;
							if(cond1.oper == hsql::kOpEquals && joined_columns[j].values[i] != cond1.whereValue) conds_fulfilled--;
							else if(cond1.oper == hsql::kOpLess || cond1.oper == hsql::kOpLessEq)
							{
								if(cond1.num_first && joined_columns[j].values[i] < cond1.whereValue) conds_fulfilled--;
								else if(!cond1.num_first && joined_columns[j].values[i] > cond1.whereValue) conds_fulfilled--;

								if(cond1.oper == hsql::kOpLess && joined_columns[j].values[i] == cond1.whereValue) conds_fulfilled--;
							}
							else if(cond1.oper == hsql::kOpGreater || cond1.oper == hsql::kOpGreaterEq)
							{
								if(cond1.num_first && joined_columns[j].values[i] > cond1.whereValue) conds_fulfilled--;
								else if(!cond1.num_first && joined_columns[j].values[i] < cond1.whereValue) conds_fulfilled--;

								if(cond1.oper == hsql::kOpGreater && joined_columns[j].values[i] == cond1.whereValue) conds_fulfilled--;
							}
						}
					}
					if(cond2.whereTable == "" || cond2.whereTable == joined_columns[j].table)
					{
						if(cond2.whereColumn == joined_columns[j].column)
						{
							if(cond2.whereTable == "") column2_cnt++;
							column2_exists = true;
							if(cond2.oper == hsql::kOpEquals && joined_columns[j].values[i] != cond2.whereValue) conds_fulfilled--;
							else if(cond2.oper == hsql::kOpLess || cond2.oper == hsql::kOpLessEq)
							{
								if(cond2.num_first && joined_columns[j].values[i] < cond2.whereValue) conds_fulfilled--;
								else if(!cond2.num_first && joined_columns[j].values[i] > cond2.whereValue) conds_fulfilled--;

								if(cond2.oper == hsql::kOpLess && joined_columns[j].values[i] == cond2.whereValue) conds_fulfilled--;
							}
							else if(cond2.oper == hsql::kOpGreater || cond2.oper == hsql::kOpGreaterEq)
							{
								if(cond2.num_first && joined_columns[j].values[i] > cond2.whereValue) conds_fulfilled--;
								else if(!cond2.num_first && joined_columns[j].values[i] < cond2.whereValue) conds_fulfilled--;

								if(cond2.oper == hsql::kOpGreater && joined_columns[j].values[i] == cond2.whereValue) conds_fulfilled--;
							}
						}
					}
				}
				if(!column1_exists)
				{
					cerr << "Error: " << cond1.whereTable;
					if(cond1.whereTable != "") cerr << ".";
					cerr << cond1.whereColumn << " does not exist.\n";
					exit(1);
				}
				if(!column2_exists)
				{
					cerr << "Error: " << cond2.whereTable;
					if(cond2.whereTable != "") cerr << ".";
					cerr << cond2.whereColumn << " does not exist.\n";
					exit(1);
				}
				if(column1_cnt > 1)
				{
					cerr << "Error: " << cond1.whereColumn << " exists in more than one table.\n";
					exit(1);
				}
				if(column2_cnt > 1)
				{
					cerr << "Error: " << cond2.whereColumn << " exists in more than one table.\n";
					exit(1);
				}
				if(whereClause->opType == hsql::kOpAnd && conds_fulfilled == 2) valid_rows.push_back(row);
				if(whereClause->opType == hsql::kOpOr && conds_fulfilled >= 1) valid_rows.push_back(row);
			}

			replace_columns(joined_columns, valid_rows);
        }        
    }

    return;
}