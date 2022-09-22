--! qt:dataset:src
set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyHiveHook;
set hive.enforce.readonly = true;

INSERT INTO src VALUES(1, 2);
