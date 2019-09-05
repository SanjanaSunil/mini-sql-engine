#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <vector>
#include "SQLParser.h"

using namespace std;

enum aggregate_functions{None, Sum, Average, Max, Min};

struct column_data
{
    string table;
    string column;
    vector<double> values;
    aggregate_functions aggr;
};

#endif