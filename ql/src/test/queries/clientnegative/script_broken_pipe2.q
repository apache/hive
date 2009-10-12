-- Tests exception in ScriptOperator.processOp() by passing extra data needed to fill pipe buffer
SELECT TRANSFORM(key, value, key, value, key, value, key, value, key, value, key, value) USING 'head -n 1' as a,b,c,d FROM src;
