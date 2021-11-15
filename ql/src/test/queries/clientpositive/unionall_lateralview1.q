drop table if exists unionall_lv_src1;
drop table if exists unionall_lv_src2;
drop table if exists unionall_lv_dest;

create table unionall_lv_src1(id int, dt string);
insert into unionall_lv_src1 values (2, '2019-04-01');

create table unionall_lv_src2( id int, dates array<string>);
insert into unionall_lv_src2 select 1 as id, array('2019-01-01','2019-01-02','2019-01-03') as dates;

create table unionall_lv_dest (id int) partitioned by (dt string);

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table unionall_lv_dest partition (dt)
select t.id, t.dt from (
select id, dt from unionall_lv_src1
union all
select id, dts as dt from unionall_lv_src2 tt lateral view explode(tt.dates) dd as dts ) t;

select * from unionall_lv_dest;

drop table unionall_lv_src1;
drop table unionall_lv_src2;
drop table unionall_lv_dest;
