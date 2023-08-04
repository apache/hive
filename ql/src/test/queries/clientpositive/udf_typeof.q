--! qt:dataset:src
DESCRIBE FUNCTION typeof;
DESCRIBE FUNCTION EXTENDED typeof;
SELECT typeof(1);
SELECT typeof("string");
SELECT typeof(CAST(1 as DECIMAL(4,3)));
SELECT typeof(key) FROM src LIMIT 2;
