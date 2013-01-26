SET hive.exec.operator.hooks=org.apache.hadoop.hive.ql.exec.TstOperatorHook;
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostTestOperatorHook;
SET hive.exec.mode.local.auto=false;
SET hive.task.progress=true;

SELECT count(1) FROM src
