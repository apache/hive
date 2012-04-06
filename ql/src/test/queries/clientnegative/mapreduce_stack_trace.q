set hive.exec.mode.local.auto=false;
set hive.exec.job.debug.capture.stacktraces=true;
set hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.VerifySessionStateStackTracesHook;

FROM src SELECT TRANSFORM(key, value) USING 'script_does_not_exist' AS (key, value);
