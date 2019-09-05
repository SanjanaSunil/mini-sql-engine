#ifndef HELPERS_H
#define HELPERS_H

#include <vector>
#include "config.h"

void print_table(vector<column_data>, bool);
void replace_columns(vector<column_data>&, vector<vector<double>>&);
void cartesian_product(vector<column_data>&);

#endif