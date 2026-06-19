--! qt:dataset:src
-- should fail: hive.fetch.task.conversion accepts none, minimal or more
desc src;

set hive.conf.validation=true;
set hive.fetch.task.conversion=true;
