SET hive.task.progress=true;
set hive.exec.operator.hooks=org.apache.hadoop.hive.ql.profiler.HiveProfiler;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.HiveProfilerResultsHook;
ADD FILE src/test/scripts/testgrep;


-- checking that script operator does not cause NPE
-- Derby strangeness is causing the output collector for the Hive Profiler to not get output during DB read 

SELECT TRANSFORM(src.key, src.value)
       USING 'testgrep' AS (tkey, tvalue)
FROM src



