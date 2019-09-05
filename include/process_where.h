#ifndef PROCESS_WHERE_H
#define PROCESS_WHERE_H

#include <string>
#include <vector>
#include <unordered_map>

#include "SQLParser.h"
#include "config.h"

using namespace std;

void process_where(vector<column_data>&, hsql::Expr*);

#endif