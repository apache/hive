set hive.optimize.constant.propagation=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table dest(key string, value string) partitioned by (ds string);

EXPLAIN
from srcpart
insert overwrite table dest partition (ds) select key, value, ds where ds='2008-04-08';

from srcpart
insert overwrite table dest partition (ds) select key, value, ds where ds='2008-04-08';
