-- SORT_QUERY_RESULTS
set hive.explain.user=false;
create table test_ice_int (a int, b string) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into test_ice_int values (11, 'ddd'), (22, 'eefe');
alter table test_ice_int set partition spec(a);
insert into test_ice_int values (33, 'rrfdfdf');
insert into table test_ice_int select * from test_ice_int;

select * from test_ice_int;
select `partition` from default.test_ice_int.partitions;
explain truncate table test_ice_int partition (a = 22);
truncate table test_ice_int partition (a = 22);
select * from test_ice_int;
select `partition` from default.test_ice_int.partitions;
explain truncate table test_ice_int partition (a = 33);
truncate table test_ice_int partition (a = 33);
select * from test_ice_int;
select `partition` from default.test_ice_int.partitions;

create table test_ice_bigint (a bigint, b string) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into test_ice_bigint values (115674892756, 'ddd'), (226784902765739, 'eefe');
alter table test_ice_bigint set partition spec(a);
insert into test_ice_bigint values (3367849937755673, 'rrfdfdf');
insert into table test_ice_bigint select * from test_ice_bigint;

select * from test_ice_bigint;
select `partition` from default.test_ice_bigint.partitions;
explain truncate table test_ice_bigint partition (a = 226784902765739);
truncate table test_ice_bigint partition (a = 226784902765739);
select * from test_ice_bigint;
select `partition` from default.test_ice_bigint.partitions;
explain truncate table test_ice_bigint partition (a = 3367849937755673);
truncate table test_ice_bigint partition (a = 3367849937755673);
select * from test_ice_bigint;
select `partition` from default.test_ice_bigint.partitions;

create table test_ice_str (a bigint, b string) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into test_ice_str values (115674892756, 'ddd'), (2267849027657399057, 'eefe');
insert into test_ice_str values (115674892756, 'a"ab'), (2267849027657399057, 'eefe');
insert into test_ice_str values (115674892756, "a'ab"), (2267849027657399057, "eefe");
alter table test_ice_str set partition spec(b);
insert into test_ice_str values (33678499377556738, 'rrfdfdf');
insert into table test_ice_str select * from test_ice_str;

select * from test_ice_str;
select `partition` from default.test_ice_str.partitions;
explain truncate table test_ice_str partition (b = 'ddd');
truncate table test_ice_str partition (b = 'ddd');
select * from test_ice_str;
select `partition` from default.test_ice_str.partitions;
explain truncate table test_ice_str partition (b = 'rrfdfdf');
truncate table test_ice_str partition (b = 'rrfdfdf');
select * from test_ice_str;
select `partition` from default.test_ice_str.partitions;
truncate table test_ice_str partition (b = 'a"ab');
truncate table test_ice_str partition (b = "a'ab");
select * from test_ice_str;
select `partition` from default.test_ice_str.partitions;

create table test_ice_date (a bigint, b date) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into test_ice_date values (115674892756, '2022-02-07'), (2267849027657399057, '2022-08-07');
alter table test_ice_date set partition spec(b);
insert into test_ice_date values (33678499377556738, '2022-08-09');

select * from test_ice_date;
select `partition` from default.test_ice_date.partitions;
explain truncate table test_ice_date partition (b = '2022-02-07');
truncate table test_ice_date partition (b = '2022-02-07');
select * from test_ice_date;
select `partition` from default.test_ice_date.partitions;
explain truncate table test_ice_date partition (b = '2022-08-09');
truncate table test_ice_date partition (b = '2022-08-09');
select * from test_ice_date;
select `partition` from default.test_ice_date.partitions;

create table test_ice_double (a double, b date) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into test_ice_double values (115674892756.67590946, '2022-02-07'), (2267849027.657399057, '2022-08-07');
alter table test_ice_double set partition spec(a);
insert into test_ice_double values (33678499.377556738, '2022-08-09');
insert into table test_ice_double select * from test_ice_double;

select * from test_ice_double;
select `partition` from default.test_ice_double.partitions;
explain truncate table test_ice_double partition (a = 115674892756.67590946);
truncate table test_ice_double partition (a = 115674892756.67590946);
select * from test_ice_double;
select `partition` from default.test_ice_double.partitions;
explain truncate table test_ice_double partition (a = 33678499.377556738);
truncate table test_ice_double partition (a = 33678499.377556738);
select * from test_ice_double;
select `partition` from default.test_ice_double.partitions;

create table test_ice_double_date (a double, b date) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into test_ice_double_date values (115674892756.67590946, '2022-02-07'), (2267849027.657399057, '2022-08-07');
alter table test_ice_double_date set partition spec(a, b);
insert into test_ice_double_date values (33678499.377556738, '2022-08-09');

select * from test_ice_double_date;
select `partition` from default.test_ice_double_date.partitions;
explain truncate table test_ice_double_date partition (a = 115674892756.67590946, b = '2022-02-07');
truncate table test_ice_double_date partition (a = 115674892756.67590946, b = '2022-02-07');
select * from test_ice_double_date;
select `partition` from default.test_ice_double_date.partitions;
explain truncate table test_ice_double_date partition (a = 33678499.377556738, b = '2022-08-09');
truncate table test_ice_double_date partition (a = 33678499.377556738, b = '2022-08-09');
select * from test_ice_double_date;
select `partition` from default.test_ice_double_date.partitions;

drop table test_ice_int;
drop table test_ice_bigint;
drop table test_ice_str;
drop table test_ice_date;
drop table test_ice_double;
drop table test_ice_double_date;
