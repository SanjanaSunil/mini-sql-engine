#ifndef PROCESS_SELECT_H
#define PROCESS_SELECT_H

#include <string>
#include <vector>

#include "SQLParser.h"
#include "config.h"

using namespace std;

void process_select(vector<column_data>&, hsql::SelectStatement*);

#endif