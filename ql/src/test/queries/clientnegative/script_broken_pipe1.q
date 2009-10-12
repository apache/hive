-- Tests exception in ScriptOperator.close() by passing to the operator a small amount of data
SELECT TRANSFORM(*) USING 'true' AS a, b, c FROM (SELECT * FROM src LIMIT 1) tmp;
