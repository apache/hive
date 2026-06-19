--! qt:dataset:srcpart
-- scanning partitioned data

create table tmptable_n1(key string, value string, hr string, ds string);

explain extended 
insert overwrite table tmptable_n1
select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08';


insert overwrite table tmptable_n1
select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08';

select * from tmptable_n1 x sort by x.key,x.value,x.ds,x.hr;

