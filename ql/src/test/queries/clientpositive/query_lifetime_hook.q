SET hive.query.lifetime.hooks=org.apache.hadoop.hive.ql.hooks.ConsoleQueryLifeTimeHook;

SELECT * FROM src LIMIT 1;
