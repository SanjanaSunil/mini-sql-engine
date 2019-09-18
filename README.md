# Mini SQL Engine

An SQL engine that runs a subset of SQL queries.

### Usage

```
$ make
$ ./a.out "SQL queries"
```

### Data Format

The tables are stored in CSV format as <table_name>.csv in the files/ directory. Only integer values in a table are supported. 

Another file in the same directory called metadata.txt has the following structure for each table:
```
<begin_table>
<table_name>
<attribute1>
    ...
<attributeN>
<end_table>
```

### Supported Queries

The following queries are supported:
* Selecting all records from one or more tables
* Projecting some number of columns from one or more tables
* Aggregate functions (sum, average, max, min)
* DISTINCT
* WHERE clause with a maximum of one AND/OR operator