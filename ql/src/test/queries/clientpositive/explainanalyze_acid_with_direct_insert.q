set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;

-- SORT_QUERY_RESULTS

drop table if exists analyze_acid_table;
create table analyze_acid_table (id int) stored as orc tblproperties('transactional'='true');

explain analyze insert into analyze_acid_table values (1),(2),(3),(4);
select * from analyze_acid_table;

insert into analyze_acid_table values (1),(2),(3),(4);
select * from analyze_acid_table;

explain analyze update analyze_acid_table set id=11 where id=1;
select * from analyze_acid_table;

update analyze_acid_table set id=22 where id=2;
select * from analyze_acid_table;

explain analyze delete from analyze_acid_table where id=3;
select * from analyze_acid_table;

delete from analyze_acid_table where id=1;
select * from analyze_acid_table;

drop table if exists analyze_acid_table;


create table tmp_table (a int, b int);
insert into tmp_table values (1, 1), (2, 1), (3, 2), (4, 3), (5, 3), (6, 3);

drop table if exists analyze_part_table;
create table analyze_part_table (a int) partitioned by (b int) stored as orc tblproperties('transactional'='true');

explain analyze insert into analyze_part_table select * from tmp_table;
select * from analyze_part_table;
show partitions analyze_part_table;

insert into analyze_part_table select * from tmp_table;
select * from analyze_part_table;
show partitions analyze_part_table;

explain analyze update analyze_part_table set a=11 where b=3;
select * from analyze_part_table;

explain analyze delete from analyze_part_table where b=1;
select * from analyze_part_table;
show partitions analyze_part_table;

update analyze_part_table set a=22 where b=3;
select * from analyze_part_table;

delete from analyze_part_table where b=1;
select * from analyze_part_table;
show partitions analyze_part_table;
drop table if exists analyze_part_table;



set hive.acid.direct.insert.enabled=false;

drop table if exists analyze_acid_table;
create table analyze_acid_table (id int) stored as orc tblproperties('transactional'='true');

explain analyze insert into analyze_acid_table values (1),(2),(3),(4);
select * from analyze_acid_table;

insert into analyze_acid_table values (1),(2),(3),(4);
select * from analyze_acid_table;

explain analyze update analyze_acid_table set id=11 where id=1;
select * from analyze_acid_table;

update analyze_acid_table set id=22 where id=2;
select * from analyze_acid_table;

explain analyze delete from analyze_acid_table where id=3;
select * from analyze_acid_table;

delete from analyze_acid_table where id=1;
select * from analyze_acid_table;

drop table if exists analyze_acid_table;


drop table if exists analyze_part_table;
create table analyze_part_table (a int) partitioned by (b int) stored as orc tblproperties('transactional'='true');

explain analyze insert into analyze_part_table select * from tmp_table;
select * from analyze_part_table;
show partitions analyze_part_table;

insert into analyze_part_table select * from tmp_table;
select * from analyze_part_table;
show partitions analyze_part_table;

explain analyze update analyze_part_table set a=11 where b=3;
select * from analyze_part_table;

explain analyze delete from analyze_part_table where b=1;
select * from analyze_part_table;
show partitions analyze_part_table;

update analyze_part_table set a=22 where b=3;
select * from analyze_part_table;

delete from analyze_part_table where b=1;
select * from analyze_part_table;
show partitions analyze_part_table;
drop table if exists analyze_part_table;
drop table tmp_table;