
# Business Signal Transformation Language: Data Flow Operations

Specifitions for pipelined data flow operations in JSON.

## Processing model

BST specifies the transformation of flows of inputs to an output model. It does this by providing relational operations over Apache Spark dataframe. Operations are connected in an directed acyclic graph with operator nodes. The shape and composition of a graph of operators can be declared in JSON.

There are two major components to BST: flow and mappings. Flow specifies the graph of sequentially executed operations on data as it is recieved transformed and emitted. 


## Data Flow Operations

Exposes dataset operations. BSTL provides operations for locating, transforming and emitting ordered sets of structured data.
The operations can be assembled together to describe a wide range of data processing pipelines.

### join

Joins two DataFrames on eqality of one or more columns

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "join"    | y        |             |   |
| joinType    | "left", "right", "inner", "outer", "semi", "anti" |         | "inner"   |   |
| cols | array of strings | y    |           |   | one or more column names  |


### union

The union of all the rows from two datasets.

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "join"    | y        |             |   |



### tee

Duplicates a source across multiple outputs

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|:-------|
| type        | "tee"  | y  |             |   |

### split

Splits a dataframe into two two outputs: the first of which the predicate expression holds true for each row, the second false


| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|:-------|
| type        | "tee"  | y  |             |   |
| expr      |  string | y   |    | a SQL expression yielding a boolean  |


### JoinToEvents

Performs a join across two timestamped inputs while limiting the joined rows from either side to the other side's relevant time window.

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "join"    | y        |             |   |
| joinType    | "left" or "right" or "inner" |         | "inner"   |   |
| cols | array of strings | y    |           |   | one or more column names for joining  |
| keyCols | array of strings | y    |           |   | one or more column names  | 
| eventTimestamp | string |            |           |   |   |
| timestamp_a | string |            |           |   |   |
| timestamp_b | string |            |           |   |   |
| eventTimestamp | string |            |           |   |   |

### partitionedAvroSource

A data source that reads a directory of Avro files, partioned by time into a DataFrame

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|:-------|
| type        | "partitionedAvroSource"  | y  |             |   |
| filePath      | URI | y       |    |   |
| srcCols | array of strings |            |           |   |   |
| cols | array of strings |            |           |   |   |
| keyCols | array of strings |            |           |   |   |
| srcTimestamp | string |            |           |   |   |
| timestamp | string |            |           |   |   |
| filter | string |            |           |   |   |

### TimedTable

A data source that reads a directory of Avro files, partioned by time into a DataFrame

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|:-------|
| type        | "timedTable"  | y  |             |   |

| srcCols | array of strings |            |           |   |   |
| cols | array of strings |            |           |   |   |
| keyCols | array of strings |            |           |   |   |
| srcTimestamp | string |            |           |   |   |
| timestamp | string |            |           |   |   |
| filter | string |            |           |   |   |


### preprocessor

A data source that reads a directory of raw csv files, resulting in timestamped rows

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|:-------|
| type        | "preprocessor"  | y  |             |   |
| readInitPath      | URI | y       |    |   |
| srcCols | array of strings |            |           |   |   |
| cols | array of strings |            |           |   |   |
| keyCols | array of strings |            |           |   |   |
| srcTimestamp | string |            |           |   |   |
| srcTimestampFormat | string |            |           |   |   |
| timestamp | string |            |           |   |   |
| filter | string |            |           |   |   |


### reader

A data source that uses a "connection" for input


| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|:-------|
| type        | "preprocessor"  | y  |             |   |
| projection.cols.from | array of strings |            |           |   |   |
| projection.cols.name | array of strings |            |           |   |   |
| keyCols | array of strings |            |           |   |   |
| srcTimestamp | string |            |           |   |   |
| srcTimestampFormat | string |            |           |   |   |
| timestamp | string |            |           |   |   |
| expr | string |      |           |   | filter rows through this expression  |

### pushStruct

pushes the named columns into a nested structure column one level

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "pushStruct"    | y        |             |   |
| cols      |  array of strings | y    |    | lists the names to go into the structure  |
| colName      | string  | y       |    | the name of the column to hold the structure |
| asArray      | boolean  |       |    | if true, the new column is an array of structures |

### popStruct

Elevates the fields of a nested structure column one level, flattening the structure.

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "popStruct"    | y        |             |   |
| col      | string  | y       |    | the name of the column to elevate |

### freshestRows

converts an event view into a table snapshot, limited to the rows with the highest timestamp for each key


| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "freshestRows"    | y        |             |   |
| keyCols | array of strings |            |           |   |   |
| timestamp | string |            |           |   |   |

### project

chooses and renames columns in a dataset

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "project"    | y        |             |   |
| projection.cols      | array of structures with "from" and "name"  | y       |    | lists the columns to be used  |

### filter

Limits the dataset to only those rows for which an expression is true

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "filter"    | y        |             |   |
| expr      |  string | y   |    | a SQL expression yielding a boolean  |


### dropColumns

Eliminates columns from a dataset

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "dropColumns"    | y        |             |   |
| cols     |  list of string | y   |    |  |


### withColumns

Adds a column to the dataset

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "withColumns"    | y        |             |   |
| cols | list of structures containing "name" and "expression" | y | |  |


### withColumnRenamed

Renames a single column

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "withColumnRenamed"    | y        |             |   |
| from      |  string | y   |    | the existing column name  |
| to      |  string | y   |    | the new column name  |

### select

Eliminates columns from a dataset. Like "project" but without ability to rename

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "select"    | y        |             |   |
| cols      | array of strings | y   |    | the names of the columns to keep  |


### deDup

Eliminates duplicate rows, choosing to keep the oldest version

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "deDup"    | y        |             |   |
| eventTimestamp        |  string         | y        |             |   |
| ignorableCols        |  array of strings         |         |             | exclude these columns when comparing  |


### sort

Re-order the rows

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "sort"    | y        |             |   |
| cols      | array of strings  | y       |    | column names to sort by |

### limit

Returns a limited number of rows

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "limit"    | y        |             |   |
| source      | string  | y       |    | name of the upstream dataset |
| rows      | integer  | y       |    | count of rows to return |


### ruleScript

Evaluates a python or javascript function ofer each row in a dataset

| property    | value     | required | default     | notes |
|:------------|:----------|:--------:|:------------|-------|
| type        | "ruleScript"    | y        |             |   |
| language        | string    |        |             |   |
| methodNames        | array of strings |        |             |   |
| paths        | array of strings |        |             |   |
| locationType        | string |        |             |   |
| base64Script        | array of strings |        |             |   |
| dependenciesFolders       | array of strings |        |             |   |

## sinks

coming soon

