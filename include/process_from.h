#ifndef PROCESS_FROM_H
#define PROCESS_FROM_H

#include <string>
#include <vector>
#include <unordered_map>

#include "SQLParser.h"

using namespace std;

void process_from(const hsql::SQLStatement*, unordered_map<string, vector<string>>&);

#endif