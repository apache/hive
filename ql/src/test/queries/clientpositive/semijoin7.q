set hive.cbo.enable=false;
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table tx1 (a integer,b integer);
insert into tx1	values (1, 105), (2, 203), (3, 300), (4, 400), (null, 400), (null, null);

create table tx2 (a int, b int);
insert into tx2 values (1, 105), (1, 1900), (2, 1995), (2, 1996), (4, 400), (4, null);

explain
select * from tx1 u left semi join tx2 v on u.a=v.a;

select * from tx1 u left semi join tx2 v on u.a=v.a;

explain
select * from tx1 u left semi join tx2 v on u.b <=> v.b;

select * from tx1 u left semi join tx2 v on u.b <=> v.b;

explain
select * from tx1 u left semi join tx2 v on u.b <> v.b;

select * from tx1 u left semi join tx2 v on u.b <> v.b;

explain
select * from tx1 u left semi join tx2 v on u.a=v.a and u.b <> v.b;

select * from tx1 u left semi join tx2 v on u.a=v.a and u.b <> v.b;

explain
select * from tx1 u left semi join tx2 v on u.a=v.a or u.b <> v.b;

select * from tx1 u left semi join tx2 v on u.a=v.a or u.b <> v.b;

explain
select * from tx1 u left semi join tx1 v on u.a=v.a;

select * from tx1 u left semi join tx1 v on u.a=v.a;

explain
select * from tx1 u left semi join tx2 v
on (u.a + v.b > 400)
  and ((case when u.a > 3 then true when v.b > 1900 then true else false end)
      or (coalesce(u.a) + coalesce(v.b) > 1900))
  and u.a = v.a;

select * from tx1 u left semi join tx2 v
on (u.a + v.b > 400)
  and ((case when u.a > 3 then true when v.b > 1900 then true else false end)
      or (coalesce(u.a) + coalesce(v.b) > 1900))
  and u.a = v.a;
