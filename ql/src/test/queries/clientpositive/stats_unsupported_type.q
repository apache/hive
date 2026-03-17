set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

-- create a table with a type that does not support column stats autogather
-- the columns before and after that column should still obtain statistics
CREATE TABLE test_stats1 (a int, b uniontype<int, string>, c int) STORED AS TEXTFILE;
INSERT INTO test_stats1 (a, b, c) VALUES (1, create_union(0, 2, ""), 3);
DESCRIBE FORMATTED test_stats1 a;
DESCRIBE FORMATTED test_stats1 b;
DESCRIBE FORMATTED test_stats1 c;

