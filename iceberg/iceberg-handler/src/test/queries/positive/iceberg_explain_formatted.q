-- A simple explain formatted test for an iceberg table to check virtual columns in the JSON output.
create external table test (a int, b int) stored by iceberg;
explain formatted select * from test;
