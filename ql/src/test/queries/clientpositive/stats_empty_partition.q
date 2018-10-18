--! qt:dataset:src
--! qt:dataset:part
-- This test verifies that writing an empty partition succeeds when
-- hive.stats.reliable is set to true.

create table tmptable_n11(key string, value string) partitioned by (part string);

set hive.stats.autogather=true;
set hive.stats.reliable=true;

insert overwrite table tmptable_n11 partition (part = '1') select * from src where key = 'no_such_value';

describe formatted tmptable_n11 partition (part = '1');
