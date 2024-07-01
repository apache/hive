--! qt:dataset:src_thrift
set hive.cbo.fallback.strategy=NEVER;
FROM src_thrift
SELECT instr('abcd', src_thrift.lintstring)
WHERE src_thrift.lintstring IS NOT NULL;
