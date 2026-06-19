--! qt:dataset:src
--! qt:dataset:part
-- This test verifies writing a query using dynamic partitions
-- which results in no partitions actually being created with
-- hive.stats.reliable set to true

create table tmptable_n7(key string) partitioned by (part string);

set hive.stats.autogather=true;
set hive.stats.reliable=true;

explain insert overwrite table tmptable_n7 partition (part) select key, value from src where key = 'no_such_value';

insert overwrite table tmptable_n7 partition (part) select key, value from src where key = 'no_such_value';
