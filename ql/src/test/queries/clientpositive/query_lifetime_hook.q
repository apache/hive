SET hive.query.lifetime.hooks=org.apache.hadoop.hive.ql.hooks.TestQueryLifeTimeHook;

SELECT * FROM src LIMIT 1;
