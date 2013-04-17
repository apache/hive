set hive.exec.operator.hooks=org.apache.hadoop.hive.ql.profiler.HiveProfiler;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.HiveProfilerResultsHook;
SET hive.task.progress=true;
FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
SELECT unioninput.*;

explain
  FROM (
    FROM src select src.key, src.value WHERE src.key < 100
    UNION ALL
    FROM src SELECT src.* WHERE src.key > 100
  ) unioninput
  SELECT unioninput.*;


