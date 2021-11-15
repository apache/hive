--! qt:dataset:srcpart
set hive.optimize.constant.propagation=true;

create table dest_n1(key string, value string) partitioned by (ds string);

EXPLAIN
from srcpart
insert overwrite table dest_n1 partition (ds) select key, value, ds where ds='2008-04-08';

from srcpart
insert overwrite table dest_n1 partition (ds) select key, value, ds where ds='2008-04-08';
