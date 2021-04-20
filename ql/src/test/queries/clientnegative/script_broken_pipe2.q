--! qt:disabled:HIVE-25036
--! qt:dataset:src
set hive.llap.execution.mode=auto;
set hive.exec.script.allow.partial.consumption = false;
-- Tests exception in ScriptOperator.processOp() by passing extra data needed to fill pipe buffer
SELECT TRANSFORM(key, value, key, value, key, value, key, value, key, value, key, value, key, value, key, value, key, value, key, value, key, value, key, value) USING 'true' as a,b,c,d FROM src;
