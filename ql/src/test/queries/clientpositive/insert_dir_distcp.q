--! qt:dataset:src
set hive.exec.copyfile.maxsize=400;

set tez.am.log.level=INFO;
set tez.task.log.level=INFO;
-- see TEZ-2931 for using INFO logging

insert overwrite directory '/tmp/src' select * from src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/src/;
