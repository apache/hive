set hive.exec.mode.local.auto=true;
set hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.VerifySessionStateLocalErrorsHook;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20)

FROM src SELECT TRANSFORM(key, value) USING 'python ../data/scripts/cat_error.py' AS (key, value);
