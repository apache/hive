--! qt:dataset:srcpart
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

show partitions srcpart;




create temporary table if not exists temp_part1_n0 like srcpart;
create temporary table if not exists temp_part2_n0 like srcpart;
describe extended temp_part1_n0;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

explain
from srcpart
insert overwrite table temp_part1_n0 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08'
insert overwrite table temp_part2_n0 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';

from srcpart
insert overwrite table temp_part1_n0 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08'
insert overwrite table temp_part2_n0 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';


show partitions temp_part1_n0;
show partitions temp_part2_n0;

select * from temp_part1_n0 where ds is not null and hr is not null;
select * from temp_part2_n0 where ds is not null and hr is not null;



