---
description: how to configure preen to read CSV files.
---

# CSV Format

Preen supports the following options for CSV format. This is largely a wrapper on the [DuckDB CSV scan options](https://duckdb.org/docs/data/csv/overview.html#parameters).

| Option               | Description                                          | Default Value |
| -------------------- | ---------------------------------------------------- | ------------- |
| all\_varchar         | Interpret all columns as varchar                     | false         |
| allow\_quoted\_nulls | Allow NULL values in quotes                          | true          |
| auto\_detect         | Automatically detect CSV dialect                     | true          |
| columns              | Specify column names                                 | -             |
| compression          | Compression type (auto, none, gzip, zstd)            | auto          |
| dateformat           | Specifies the date format to use                     | -             |
| decimal\_separator   | Specifies the decimal separator                      | .             |
| delim                | Specifies the delimiter character                    | ,             |
| escape               | Specifies the escape character                       | "             |
| filename             | Include filename in the result                       | false         |
| force\_not\_null     | Do not convert blank values to NULL                  | \[]           |
| header               | Whether or not the CSV file has a header             | false         |
| ignore\_errors       | Ignore parsing errors                                | false         |
| max\_line\_size      | Maximum line size in bytes                           | 2097152       |
| names                | Specify column names                                 | -             |
| new\_line            | Specifies the newline character                      | -             |
| normalize\_names     | Normalize column names                               | false         |
| null\_padding        | Pad columns with null values if row is too short     | false         |
| nullstr              | Specifies the string that represents NULL values     | -             |
| parallel             | Use multi-threading for reading CSV files            | true          |
| quote                | Specifies the quote character                        | "             |
| sample\_size         | Number of sample rows for dialect and type detection | 20480         |
| skip                 | Number of rows to skip                               | 0             |
| timestampformat      | Specifies the timestamp format                       | -             |
| types                | Specify column types                                 | -             |
| union\_by\_name      | Union by name when reading multiple files            | false         |

## Examples

### Basic Auto-Detection

This is the most common case. Preen will auto-detect the CSV format and use the default options.

```yaml
# FILENAME: ~/.preen/models/users.yaml
name: users
type: file
file_patterns:
  - "users/v1/**.csv" # This will match all csv files under the users/v1 prefix
format: csv
options:
  auto_detect: true
  header: true
  delim: ","
  quote: "\""
  escape: "\""
  union_by_name: true
```

### Fully Specifying Options without auto-detection

This is useful if you want to override the auto-detection and specify the options manually. This will save time and avoid the memory overhead of auto-detection.

```yaml
# FILENAME: ~/.preen/models/users.yaml
name: users
type: file
file_patterns:
  - "users/v1/**.csv"
format: csv
options:
  auto_detect: false
  header: true
  delim: ","
  quote: "\""
  escape: "\""
  columns: # List of all columns in the CSV file along with their DuckDB types
    - name: id
      type: bigint
    - name: name
      type: varchar
    - name: email
      type: varchar
    - name: birthday
      type: date
```

### Partially Specifying Options to override auto-detection

```yaml
# FILENAME: ~/.preen/models/users.yaml
name: users
type: file
file_patterns:
  - "users/v1/**.csv"
format: csv
options:
  auto_detect: true
  header: true
  delim: ","
  quote: "\""
  escape: "\""
  types: # This overrides the DuckDB auto-detection for the specified columns
    - name: birthday
      type: date
```
