# Validation

The validation step ensures that the column types defined across all sources using a shared model are consistent. This is managed via the [Boyer-Moore Majority Voting](https://en.wikipedia.org/wiki/Boyer%E2%80%93Moore\_majority\_vote\_algorithm) algorithm whereby the minority columns are coerced into the data type of the majority data type. If this fails (e.g. an `id` column coerced from a `string` to an `int` contains an alphabetic character) the validation process fails.
