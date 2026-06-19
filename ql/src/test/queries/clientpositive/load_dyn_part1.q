--! qt:dataset:srcpart
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

show partitions srcpart;




create table if not exists nzhang_part1_n0 like srcpart;
create table if not exists nzhang_part2_n0 like srcpart;
describe extended nzhang_part1_n0;

set hive.exec.dynamic.partition=true;

explain
from srcpart
insert overwrite table nzhang_part1_n0 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08'
insert overwrite table nzhang_part2_n0 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';

from srcpart
insert overwrite table nzhang_part1_n0 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08'
insert overwrite table nzhang_part2_n0 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';


show partitions nzhang_part1_n0;
show partitions nzhang_part2_n0;

select * from nzhang_part1_n0 where ds is not null and hr is not null;
select * from nzhang_part2_n0 where ds is not null and hr is not null;



