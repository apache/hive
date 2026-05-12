set hive.fetch.task.conversion=none;
set hive.explain.user=false;

create external table tbl_ice_pp_key(a int, b string) stored by iceberg;

insert into tbl_ice_pp_key values (1, 'one'), (2, 'two');
alter table tbl_ice_pp_key create tag s1;

insert into tbl_ice_pp_key values
  (3, 'three'), (4, 'four'), (5, 'five'),
  (6, 'six'), (7, 'seven'),(8, 'eight'),
  (9, 'nine'),  (10, 'ten');

explain select count(*) from tbl_ice_pp_key;
select count(*) from tbl_ice_pp_key;

explain select count(*) from tbl_ice_pp_key for system_version as of 's1';
select count(*) from tbl_ice_pp_key for system_version as of 's1';

explain
select cur.cnt as cur_cnt, snap.cnt as snap_cnt
from (
  select count(*) as cnt from tbl_ice_pp_key
) cur
cross join (
  select count(*) as cnt from tbl_ice_pp_key for system_version as of 's1'
) snap;

select cur.cnt as cur_cnt, snap.cnt as snap_cnt
from (
  select count(*) as cnt from tbl_ice_pp_key
) cur
cross join (
  select count(*) as cnt from tbl_ice_pp_key for system_version as of 's1'
) snap;

-- with a partition predicate
explain
select 'current' as ver, count(*) as cnt from tbl_ice_pp_key where a > 0
union all
select 'asof_s1' as ver, count(*) as cnt from tbl_ice_pp_key for system_version as of 's1'
where a > 0;

select 'current' as ver, count(*) as cnt from tbl_ice_pp_key where a > 0
union all
select 'asof_s1' as ver, count(*) as cnt from tbl_ice_pp_key for system_version as of 's1'
where a > 0;

drop table tbl_ice_pp_key;