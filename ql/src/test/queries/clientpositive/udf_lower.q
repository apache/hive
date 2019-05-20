--! qt:dataset:src
set hive.mapred.mode=nonstrict;
DESCRIBE FUNCTION lower;
DESCRIBE FUNCTION EXTENDED lower;

EXPLAIN
SELECT lower('AbC 123'), upper('AbC 123') FROM src WHERE key = 86;

SELECT lower('AbC 123'), upper('AbC 123') FROM src WHERE key = 86;
