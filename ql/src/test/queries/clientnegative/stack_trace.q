--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
set hive.exec.mode.local.auto=false;
set hive.exec.job.debug.capture.stacktraces=true;
set hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.VerifySessionStateStackTracesHook;

FROM src SELECT TRANSFORM(key, value) USING 'script_does_not_exist' AS (key, value);

-- Hadoop 0.23 changes the getTaskDiagnostics behavior
-- The Error Code of hive failure MapReduce job changes
-- In Hadoop 0.20
-- Hive failure MapReduce job gets 20000 as Error Code
-- In Hadoop 0.23
-- Hive failure MapReduce job gets 2 as Error Code
