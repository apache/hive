SET hive.query.lifetime.hooks=org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookTest;

SELECT * FROM src LIMIT 1;
