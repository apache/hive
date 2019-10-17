--set hive.explain.user=false;
--set hive.optimize.reducededuplication=false;
set hive.cbo.enable=false;
--explain
--set hive.optimize.groupby=false;
-- set hive.map.groupby.sorted=false;
--set hive.optimize.countdistinct=false;


drop table if exists xl1;
create table xl1 as
select '1' as mdl_yr_desc, 2 as seq_no,'3' as opt_desc1,4 as opt_desc,1 as row_num;

explain
select trim(base.mdl_yr_desc) mdl_yr_desc, trim(base.opt_desc) opt_desc
from
(
    SELECT trim(mdl_yr_desc) mdl_yr_desc, concat_ws(' ', collect_set(trim(opt_desc1))) AS opt_desc
    from
    (
        select t14304.* 
        from
        (
            select * from xl1
        ) t14304  
        where row_num = 1
        order by trim(mdl_yr_desc), cast(seq_no as int) asc
    ) x
    group by trim(mdl_yr_desc)
) base
inner join
    (
        select 1 as v
    ) dedup
    on  trim(base.mdl_yr_desc) != dedup.v
group by trim(base.mdl_yr_desc), trim(base.opt_desc) ;


select trim(base.mdl_yr_desc) mdl_yr_desc, trim(base.opt_desc) opt_desc
from
(
    SELECT trim(mdl_yr_desc) mdl_yr_desc, concat_ws(' ', collect_set(trim(opt_desc1))) AS opt_desc
    from
    (
        select t14304.* 
        from
        (
            select * from xl1
        ) t14304  
        where row_num = 1
        order by trim(mdl_yr_desc), cast(seq_no as int) asc
    ) x
    group by trim(mdl_yr_desc)
) base
inner join
    (
        select 1 as v
    ) dedup
    on  trim(base.mdl_yr_desc) != dedup.v
group by trim(base.mdl_yr_desc), trim(base.opt_desc) ;

