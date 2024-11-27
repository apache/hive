set hive.cli.print.header=true;

create table tmp1 (a int, b string);
create table tmp2 (a int, b string);
insert into table tmp1 values (3, "a");

explain cbo
select count(*) from tmp1;
select count(*) from tmp1;

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);

set hive.cbo.rule.exclusion.regex=;

explain cbo
select count(*) from tmp1;
select count(*) from tmp1;

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);
