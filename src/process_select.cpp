#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>

#include "SQLParser.h"
#include "config.h"
#include "helpers.h"

using namespace std;

void process_select(vector<column_data>& joined_columns, hsql::SelectStatement* sel) {

    vector<column_data> select_columns;
    for(int i = 0; i < (int) sel->selectList->size(); ++i)
    {
        column_data temp;
        temp.table = "";
        temp.aggr = None;

        if((*sel->selectList)[i]->type == hsql::kExprFunctionRef)
        {
            if((*sel->selectList)[i]->exprList == NULL || (*sel->selectList)[i]->exprList->size() != 1 || (*(*sel->selectList)[i]->exprList)[0]->type != 6)
            {
                fprintf(stderr, "Error: Cannot apply aggregation function.\n");
                exit(1);
            }

            if((*(*sel->selectList)[i]->exprList)[0]->table) temp.table = (*(*sel->selectList)[i]->exprList)[0]->table;
            temp.column = (*(*sel->selectList)[i]->exprList)[0]->getName();

            string aggr_funct = (*sel->selectList)[i]->getName();
            transform(aggr_funct.begin(), aggr_funct.end(), aggr_funct.begin(), ::toupper);
            if(aggr_funct == "SUM") temp.aggr = Sum;
            else if(aggr_funct == "AVERAGE") temp.aggr = Average;
            else if(aggr_funct == "MAX") temp.aggr = Max;
            else if(aggr_funct == "MIN") temp.aggr = Min;
            else
            {
                fprintf(stderr, "Error: Unknown function.\n");
                exit(1);
            }
        }
        else if((*sel->selectList)[i]->type == hsql::kExprStar)
        {
            if((*sel->selectList)[i]->table) temp.table = (*sel->selectList)[i]->table;
            temp.column = "*";
        }
        else if((*sel->selectList)[i]->type == hsql::kExprColumnRef)
        {
            if((*sel->selectList)[i]->table) temp.table = (*sel->selectList)[i]->table;
            temp.column = (*sel->selectList)[i]->getName();
        }
        else
        {
            fprintf(stderr, "Error: Invalid SQL query.\n");
            exit(1);
        }

        select_columns.push_back(temp);
    }


    for(int i = 1; i <(int) select_columns.size(); ++i)
    {
        if(select_columns[i].aggr == None || select_columns[i-1].aggr == None)
        {
            if(select_columns[i].aggr != select_columns[i-1].aggr)
            {
                cerr << "Error: Aggregation not uniformly applied.\n";
                exit(1);
            }
        }
    }

    string where_table1 = "";
    string where_table2 = "";
    string where_column1 = "";
    string where_column2 = "";

    if(sel->whereClause && sel->whereClause->opType == hsql::kOpEquals)
    {
        if(sel->whereClause->expr->type == hsql::kExprColumnRef && sel->whereClause->expr2->type == hsql::kExprColumnRef)
        {
            if(sel->whereClause->expr->table) where_table1 = sel->whereClause->expr->table;
            if(sel->whereClause->expr2->table) where_table2 = sel->whereClause->expr2->table;
            where_column1 = sel->whereClause->expr->name;
            where_column2 = sel->whereClause->expr2->name;
        }
    }

    bool join_display = true;
    vector<column_data> final_columns;
    for(int i = 0; i < (int) select_columns.size(); ++i)
    {
        bool column_exists = false;
        int column_count = 0;
        for(int j = 0; j < (int) joined_columns.size(); ++j)
        {
            if(select_columns[i].table == "" || select_columns[i].table == joined_columns[j].table)
            {
                if(select_columns[i].column == "*" || select_columns[i].column == joined_columns[j].column)
                {
                    if(select_columns[i].table == "" && select_columns[i].column != "*") column_count++;
                    column_exists = true;

                    column_data temp;
                    temp.table = joined_columns[j].table;
                    temp.column = joined_columns[j].column;
                    temp.aggr = select_columns[i].aggr;
                    temp.values = joined_columns[j].values;
                    final_columns.push_back(temp);

                    bool tables_empty = (select_columns[i].table == "" || where_table1 == "" || where_table2 == "");
                    bool tables_equal = (select_columns[i].table == where_table1 || select_columns[i].table == where_table2);
                    if(tables_empty || tables_equal)
                    {
                        if(joined_columns[j].column == where_column1 || joined_columns[j].column == where_column2)
                        {
                            if(join_display) join_display = false;
                            else final_columns.pop_back();
                        }
                    }
                }
            }
        }
        if(!column_exists)
        {
            std::cerr << "Error: " << select_columns[i].table;
            if(select_columns[i].table != "") std::cerr << ".";
            std::cerr << select_columns[i].column << " does not exist.\n";
            exit(1);
        }
        if(column_count > 1)
        {
            std::cerr << "Error: " << select_columns[i].column << " exists in more than one table.\n";
            exit(1);
        }
    }

    if(final_columns[0].values.size() > 0 && final_columns[0].aggr != None)
    {
        for(int i = 0; i < (int) final_columns.size(); ++i)
        {
            double aggr_val = 0;
            if(final_columns[i].aggr == Sum || final_columns[i].aggr == Average)
            {
                for(int j = 0; j < (int) final_columns[i].values.size(); ++j) aggr_val += final_columns[i].values[j];
                if(final_columns[i].aggr == Average) aggr_val /= final_columns[i].values.size();
            }
            else if(final_columns[i].aggr == Max)
            {
                aggr_val = *max_element(final_columns[i].values.begin(), final_columns[i].values.end());
            }
            else if(final_columns[i].aggr == Min)
            {
                aggr_val = *min_element(final_columns[i].values.begin(), final_columns[i].values.end());
            }

            final_columns[i].values.clear();
            final_columns[i].values.push_back(aggr_val);
        }
    }

    print_table(final_columns, sel->selectDistinct);

    return;
}