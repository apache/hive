-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table acidtlb(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table othertlb(c int, d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into acidtlb values(10,200),(30,500);
insert into othertlb values(10, 21),(30, 22),(60, 23),(70, 24),(80, 25);


explain cbo
select a, 6 as c, b from acidtlb sort by a, c, b;
select a, 6 as c, b from acidtlb sort by a, c, b;

explain cbo
update acidtlb set b=777;
update acidtlb set b=777;

select * from acidtlb;


explain cbo
update acidtlb set b=350
where a in (select a from acidtlb where a = 30);
update acidtlb set b=350
where a in (select a from acidtlb where a = 30);

select * from acidtlb;

explain cbo
update acidtlb set b=450
where a in (select c from othertlb where c < 65);
update acidtlb set b=450
where a in (select c from othertlb where c < 65);

select * from acidtlb;

explain cbo
delete from acidtlb
where a in (
    select a from acidtlb a
             join othertlb o on a.a = o.c
             where o.d = 21);
delete from acidtlb
where a in (
    select a from acidtlb a
             join othertlb o on a.a = o.c
             where o.d = 21);

select * from acidtlb order by a;

explain cbo
merge into acidtlb as t using othertlb as s on t.a = s.c
when matched and s.c < 60 then delete
when matched and s.c = 60 then update set b = 1000
when not matched then insert values (s.c, 2000 + s.d);

merge into acidtlb as t using othertlb as s on t.a = s.c
when matched and s.c < 30 then delete
when matched and s.c = 30 then update set b = 1000
when not matched then insert values (s.c, 2000 + s.d);

select * from acidtlb;
