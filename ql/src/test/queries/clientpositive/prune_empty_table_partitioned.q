set hive.cli.print.header=true;

create table tmp1 (a int, b string) partitioned by (c int);
create table tmp2 (a int, b string) partitioned by (c int);
create table tmp3 (a int, b string) partitioned by (c int);

insert into table tmp1 partition (c = 30) values (3, "a"), (4, "b"), (5, "c");
insert into table tmp1 partition (c = 40) values (6, "d");
insert into table tmp2 partition (c = 30) values (7, "e");

explain cbo
select count(*) from tmp1;
select count(*) from tmp1;

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

explain cbo
select count(*) from tmp3;
select count(*) from tmp3;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);

explain cbo
select * from tmp1 join tmp2 using(c) where tmp1.c=30;
select * from tmp1 join tmp2 using(c) where tmp1.c=30;

explain cbo
select * from tmp1 join tmp2 using(c) where tmp1.c=40;
select * from tmp1 join tmp2 using(c) where tmp1.c=40;

set hive.prune.empty.tables.in.test=true;

explain cbo
select count(*) from tmp1;
select count(*) from tmp1;

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

explain cbo
select count(*) from tmp3;
select count(*) from tmp3;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);

explain cbo
select * from tmp1 join tmp2 using(c) where tmp1.c=30;
select * from tmp1 join tmp2 using(c) where tmp1.c=30;

explain cbo
select * from tmp1 join tmp2 using(c) where tmp1.c=40;
select * from tmp1 join tmp2 using(c) where tmp1.c=40;

set hive.cbo.rule.exclusion.regex=HivePruneZeroRowsTable;

explain cbo
select count(*) from tmp1;
select count(*) from tmp1;

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

explain cbo
select count(*) from tmp3;
select count(*) from tmp3;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);

explain cbo
select * from tmp1 join tmp2 using(c) where tmp1.c=30;
select * from tmp1 join tmp2 using(c) where tmp1.c=30;

explain cbo
select * from tmp1 join tmp2 using(c) where tmp1.c=40;
select * from tmp1 join tmp2 using(c) where tmp1.c=40;
