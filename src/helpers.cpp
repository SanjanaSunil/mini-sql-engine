#include <iostream>
#include <string>
#include <vector>

void join(std::vector<std::vector<double>>& out, std::vector<std::vector<double>>& inp, std::vector<int> ids, std::vector<int> prev_ind, std::vector<double>& cur, int ind) {

    if(ind >= (int) inp.size())
    {
        out.push_back(cur);
        return;
    }

    if(prev_ind[ids[ind]] == -1)
    {
        for(int i = 0; i < (int) inp[ind].size(); ++i)
        {
            prev_ind[ids[ind]] = i;
            cur.push_back(inp[ind][i]);
            join(out, inp, ids, prev_ind, cur, ind+1);
            cur.pop_back();
            prev_ind[ids[ind]] = -1;
        }
    }
    else
    {
        cur.push_back(inp[ind][prev_ind[ids[ind]]]);
        join(out, inp, ids, prev_ind, cur, ind+1);
        cur.pop_back();
    }
    
    return;
}

std::vector<std::vector<double>> cartesian_product(std::vector<std::string> tables, std::vector<std::vector<double>> inp) {

    std::vector<int> ids;
    int id = 0;
    for(int i = 0; i < (int) inp.size(); ++i)
    {
        int j = i - 1;
        while(j >= 0 && tables[i] != tables[j]) j--;
        if(j == -1)
        {
            ids.push_back(id);
            id++;
        }
        else ids.push_back(ids[j]);
    }

    std::vector<int> prev_ind(tables.size()+1, -1);

    std::vector<std::vector<double>> out;
    std::vector<double> cur;

    join(out, inp, ids, prev_ind, cur, 0);

    return out;
}
