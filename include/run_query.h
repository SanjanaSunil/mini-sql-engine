#ifndef RUN_QUERY_H
#define RUN_QUERY_H

enum aggregate_functions{None, Sum, Average, Max, Min};
typedef std::vector<std::pair<std::pair<std::string, std::string>, enum aggregate_functions>> COLUMN_MAP;

void select(std::unordered_map<std::string, std::vector<std::string>>&, std::vector<std::string>&, COLUMN_MAP&, hsql::Expr*);

#endif