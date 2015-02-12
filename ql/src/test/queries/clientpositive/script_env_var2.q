set hive.script.operator.id.env.var = MY_ID;
-- Same test as script_env_var1, but test setting the variable name
SELECT count(1) FROM
( SELECT * FROM (SELECT TRANSFORM('echo $MY_ID') USING 'sh' AS key FROM src LIMIT 1)a UNION ALL
  SELECT * FROM (SELECT TRANSFORM('echo $MY_ID') USING 'sh' AS key FROM src LIMIT 1)b ) a GROUP BY key;
