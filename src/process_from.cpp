#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>

#include "SQLParser.h"
#include "read_files.h"
#include "config.h"
#include "helpers.h"
#include "process_where.h"
#include "process_select.h"

using namespace std;

void process_from(const hsql::SQLStatement* query, unordered_map<string,vector<string>>& tables_columns) {

    hsql::SelectStatement* sel = (hsql::SelectStatement*) query;

    // Get table names (FROM)
    vector<string> tables;
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
            for(int j = 0; j < (int) tables.size(); ++j)
            {
                if(i != j && tables[i] == tables[j])
                {
                    cerr << "Error: Not unique table: " << tables[i] << ".\n";
                    exit(1);
                }
            }
        }
    }

    vector<column_data> all_columns;
    for(int i = 0; i < (int) tables.size(); ++i)
    {
        if(tables_columns.find(tables[i]) == tables_columns.end())
        {
            cerr << "Error: Table " << tables[i] << " does not exist.\n";
            exit(1);
        }
        for(int j = 0; j < (int) tables_columns[tables[i]].size(); ++j)
        {
            column_data temp;
            temp.table = tables[i];
            temp.column = tables_columns[tables[i]][j];
            temp.values = read_table_column(tables[i], j);
            temp.aggr = None;
            all_columns.push_back(temp);
        }
    }

    cartesian_product(all_columns);

    process_where(all_columns, sel->whereClause);
    process_select(all_columns, sel);

    return;
}
