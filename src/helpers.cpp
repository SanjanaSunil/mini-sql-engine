#include <iostream>
#include <string>
#include <vector>

#include "config.h"

using namespace std;

void print_table(vector<column_data> final_columns, bool distinct) {

    for(int i = 0; i < (int) final_columns.size(); ++i)
    {
        if(i != 0) cout << ',';
        if(final_columns[i].aggr == Sum) cout << "SUM(";
        else if(final_columns[i].aggr == Average) cout << "AVERAGE(";
        else if(final_columns[i].aggr == Max) cout << "MAX(";
        else if(final_columns[i].aggr == Min) cout << "MIN(";

        cout << final_columns[i].table << '.' << final_columns[i].column;

        if(final_columns[i].aggr != None) cout << ')';
    }
    cout << endl;

    for(int i = 0; i < (int) final_columns[0].values.size(); ++i)
    {
        bool duplicate = false;
        for(int j = i-1; j >= 0; --j)
        {
            duplicate = true;
            for(int k = 0; k < (int) final_columns.size(); ++k)
            {
                if(final_columns[k].values[i] != final_columns[k].values[j])
                {
                    duplicate = false;
                    break;
                }
            }
            if(duplicate == true) break;
        }
        if(!duplicate)
        {
            for(int j = 0; j < (int) final_columns.size(); ++j)
            {
                if(j != 0) cout << ',';
                cout << final_columns[j].values[i];
            }
            cout << endl;
        }
    }

    return;
}


void replace_columns(vector<column_data>& old_columns, vector<vector<double>>& new_rows) {
    
    for(int i = 0; i < (int) old_columns.size(); ++i) old_columns[i].values.clear();
    for(int i = 0; i < (int) new_rows.size(); ++i) 
    {
        for(int j = 0; j < (int) new_rows[i].size(); ++j) 
        {
            old_columns[j].values.push_back(new_rows[i][j]);
        }
    }

    return;
}

void join(vector<vector<double>>& out, vector<column_data>& inp, vector<double>& cur, int ind, string prev_table, int prev_ind) {

    if(ind >= (int) inp.size())
    {
        out.push_back(cur);
        return;
    }

    if(inp[ind].table != prev_table)
    {
        for(int i = 0; i < (int) inp[ind].values.size(); ++i)
        {
            cur.push_back(inp[ind].values[i]);
            join(out, inp, cur, ind+1, inp[ind].table, i);
            cur.pop_back();
        }
    }
    else
    {
        cur.push_back(inp[ind].values[prev_ind]);
        join(out, inp, cur, ind+1, inp[ind].table, prev_ind);
        cur.pop_back();
    }
    

    return;
}

void cartesian_product(vector<column_data>& all_columns) {

    vector<vector<double>> out;
    vector<double> cur;

    join(out, all_columns, cur, 0, "", 0);
    replace_columns(all_columns, out);

    return;
}
