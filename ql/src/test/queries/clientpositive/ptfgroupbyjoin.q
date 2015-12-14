set hive.mapred.mode=nonstrict;
create table tlb1 (id int, fkey int, val string);
create table tlb2 (fid int, name string);
insert into table tlb1 values(100,1,'abc');
insert into table tlb1 values(200,1,'efg');
insert into table tlb2 values(1, 'key1');

explain 
select ddd.id, ddd.fkey, aaa.name
from (
    select id, fkey, 
    row_number() over (partition by id, fkey) as rnum
    from tlb1 group by id, fkey
 ) ddd 
inner join tlb2 aaa on aaa.fid = ddd.fkey;

select ddd.id, ddd.fkey, aaa.name
from (
    select id, fkey, 
    row_number() over (partition by id, fkey) as rnum
    from tlb1 group by id, fkey
 ) ddd 
inner join tlb2 aaa on aaa.fid = ddd.fkey;

explain
select ddd.id, ddd.fkey, aaa.name, ddd.rnum
from (
    select id, fkey,
    row_number() over (partition by id, fkey) as rnum
    from tlb1 group by id, fkey
 ) ddd
inner join tlb2 aaa on aaa.fid = ddd.fkey;

select ddd.id, ddd.fkey, aaa.name, ddd.rnum
from (
    select id, fkey,
    row_number() over (partition by id, fkey) as rnum
    from tlb1 group by id, fkey
 ) ddd
inner join tlb2 aaa on aaa.fid = ddd.fkey;


set hive.optimize.ppd=false;

explain 
select ddd.id, ddd.fkey, aaa.name
from (
    select id, fkey,
    row_number() over (partition by id, fkey) as rnum
    from tlb1 group by id, fkey
 ) ddd
inner join tlb2 aaa on aaa.fid = ddd.fkey;

select ddd.id, ddd.fkey, aaa.name
from (
    select id, fkey, 
    row_number() over (partition by id, fkey) as rnum
    from tlb1 group by id, fkey
 ) ddd 
inner join tlb2 aaa on aaa.fid = ddd.fkey;


