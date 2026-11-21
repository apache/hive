-- SharedWorkOptimizer must not merge two scans if removing the discarded scan's operators leaves a
-- work without a ReduceSink/FileSink. Here the merge used to delete the only ReduceSink of a
-- reducer whose remaining output was a dynamic partition pruning event. GenTezWork then did not
-- create a vertex for that reducer, its mergepartial GroupBy ended up in the upstream Map vertex,
-- and the query failed with "cannot find field key from [0:_col0, 1:_col1, 2:_col2]".

set hive.auto.convert.join=true;
set hive.stats.column.autogather=false;
set hive.tez.dynamic.partition.pruning.max.data.size=1000000;

create table ctrl (ctrl_dt string);
create table event_hist (user_id string) partitioned by (biz_dt string);

insert into ctrl values ('2026-10-07');
insert into event_hist partition (biz_dt='2026-10-07') values ('u1'), ('u2'), (null);

alter table event_hist partition (biz_dt='2026-10-07') update statistics set('numRows'='23911','rawDataSize'='104884811');

explain
with prc_sts as (
  select distinct a.ctrl_dt data_dt, b.prc_dttm, b.prc_user
  from ctrl a left join (select 'x' prc_dttm, 'y' prc_user) b on 'A' = 'A'
),
eom_dt as (
  select biz_dt eom_dt from event_hist where biz_dt in (select data_dt from prc_sts)
),
pop as (
  select e.user_id, e.biz_dt
  from event_hist e
  where e.biz_dt in (select eom_dt from eom_dt) and e.user_id is null
  union all
  select cast(count(*) as string), e.biz_dt
  from event_hist e
  where e.biz_dt in (select eom_dt from eom_dt) and e.user_id is not null
  group by e.biz_dt
)
select count(*), p.prc_dttm, p.prc_user, d.biz_dt
from pop d left join prc_sts p on 'A' = 'A'
group by d.biz_dt, p.prc_dttm, p.prc_user;

with prc_sts as (
  select distinct a.ctrl_dt data_dt, b.prc_dttm, b.prc_user
  from ctrl a left join (select 'x' prc_dttm, 'y' prc_user) b on 'A' = 'A'
),
eom_dt as (
  select biz_dt eom_dt from event_hist where biz_dt in (select data_dt from prc_sts)
),
pop as (
  select e.user_id, e.biz_dt
  from event_hist e
  where e.biz_dt in (select eom_dt from eom_dt) and e.user_id is null
  union all
  select cast(count(*) as string), e.biz_dt
  from event_hist e
  where e.biz_dt in (select eom_dt from eom_dt) and e.user_id is not null
  group by e.biz_dt
)
select count(*), p.prc_dttm, p.prc_user, d.biz_dt
from pop d left join prc_sts p on 'A' = 'A'
group by d.biz_dt, p.prc_dttm, p.prc_user;
