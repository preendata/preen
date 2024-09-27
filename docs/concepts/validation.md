---
description: how is data validated?
---

# Validation

## Overview

When collating data from multiple sources, it is possible that the data types of the columns do not match. For example, a column may be defined as a `string` in one source and as an `int` in another. Preen will attempt to coerce the data types of the columns to the most common data type across all sources. We do this by implementing a [majority voting algorithm](https://en.wikipedia.org/wiki/Boyer%E2%80%93Moore_majority_vote_algorithm). If we are unable to determine the data type of a column, we will error out and require manual intervention.

**Note:** There will be cases where you need to manually cast the data types of the columns in your model.

We store the results of the validation step in a DuckDB table called `preen_information_schema`. You can use this table to inspect the results of the validation step and to cast the data types of the columns in your model.

## CLI Commmands

```bash
preen source validate
```

## Code References

- [metadata.go](https://github.com/preendata/preen/blob/main/internal/engine/metadata.go)
- [columns.go](https://github.com/preendata/preen/blob/main/internal/engine/columns.go)
