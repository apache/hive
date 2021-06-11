select count(1) 
from (
  select 1 as c1, array(1, 2, 3) as c2
  union all
  select 2 as c1, array(2, 3, 4) as c2
) T
lateral view explode(c2) LV as c
where c = 42
and T.c1 not in (select 1 union all select 3);

create table service_stat_log(
  logitems array<struct<dsp:string,iswin:boolean,adid:int,triggerId:string>>
);
create table ad_info(adid int, subaccountid int);

insert into table service_stat_log values
  (array(named_struct('dsp', 'delivery', 'iswin', true, 'adid', 1, 'triggerId', 'a'))),
  (array(named_struct('dsp', 'ocpa', 'iswin', true, 'adid', 2, 'triggerId', 'b'))),
  (array(named_struct('dsp', 'ocpa', 'iswin', false, 'adid', 3, 'triggerId', 'c'))),
  (array(named_struct('dsp', 'other', 'iswin', true, 'adid', 4, 'triggerId', 'd'))),
  (array(named_struct('dsp', 'other', 'iswin', false, 'adid', 5, 'triggerId', 'e')));

insert into table ad_info values
  (1, 16010),
  (2, 14863),
  (3, 16010),
  (4, 14863),
  (5, 16010);

explain select count(distinct logItem.triggerId)
from service_stat_log LATERAL VIEW explode(logItems) LogItemTable AS logItem
where logItem.dsp in ('delivery', 'ocpa')
and logItem.iswin = true
and logItem.adid in (
    select distinct adId
    from
        ad_info
    where
        subAccountId in (16010, 14863));

select count(distinct logItem.triggerId)
from service_stat_log LATERAL VIEW explode(logItems) LogItemTable AS logItem
where logItem.dsp in ('delivery', 'ocpa')
and logItem.iswin = true
and logItem.adid in (
    select distinct adId
    from
        ad_info
    where
        subAccountId in (16010, 14863));

drop table service_stat_log;
drop table ad_info;
