set hive.exec.script.allow.partial.consumption = true;
-- Test to ensure that a script with a bad error code still fails even with partial consumption
SELECT TRANSFORM(*) USING 'false' AS a, b, c FROM (SELECT * FROM src LIMIT 1) tmp;
