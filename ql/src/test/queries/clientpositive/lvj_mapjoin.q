--! qt:dataset:alltypesorc
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

drop table sour1;
drop table sour2;
drop table expod1;
drop table expod2;

set hive.auto.convert.join=true;

create table sour1(id int, av1 string, av2 string, av3 string) row format delimited fields terminated by ',';
create table sour2(id int, bv1 string, bv2 string, bv3 string) row format delimited fields terminated by ',';

load data local inpath '../../data/files/sour1.txt' into table sour1;
load data local inpath '../../data/files//sour2.txt' into table sour2;

create table expod1(aid int, av array<string>);
create table expod2(bid int, bv array<string>);

insert overwrite table expod1 select id, array(av1,av2,av3) from sour1;
insert overwrite table expod2 select id, array(bv1,bv2,bv3) from sour2;

explain with sub1 as
(select aid, avalue from expod1 lateral view explode(av) avs as avalue ),
sub2 as
(select bid, bvalue from expod2 lateral view explode(bv) bvs as bvalue)
select sub1.aid, sub1.avalue, sub2.bvalue
from sub1,sub2
where sub1.aid=sub2.bid;

with sub1 as
(select aid, avalue from expod1 lateral view explode(av) avs as avalue ),
sub2 as
(select bid, bvalue from expod2 lateral view explode(bv) bvs as bvalue)
select sub1.aid, sub1.avalue, sub2.bvalue
from sub1,sub2
where sub1.aid=sub2.bid;

create temporary table tmp_lateral_view(
                      arst array<struct<age:int,name:string>>
                    ) stored as orc;
insert into table tmp_lateral_view
                    select array(named_struct('age',cint,'name',cstring1))
                    from alltypesorc limit 10;
select arst.name, arst.age
                    from tmp_lateral_view
                    lateral view inline(arst) arst;