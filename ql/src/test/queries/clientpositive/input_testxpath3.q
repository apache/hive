--! qt:dataset:src_thrift
set hive.cbo.fallback.strategy=NEVER;
EXPLAIN
FROM src_thrift
SELECT src_thrift.mstringstring['key_9'], src_thrift.lintstring.myint;

FROM src_thrift
SELECT src_thrift.mstringstring['key_9'], src_thrift.lintstring.myint;
