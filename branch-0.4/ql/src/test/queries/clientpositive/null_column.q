drop table temp_null;
drop table tt;
drop table tt_b;

create table temp_null(a int) stored as textfile;
load data local inpath '../data/files/test.dat' overwrite into table temp_null;

select null, null from temp_null;

create table tt(a int, b string);
insert overwrite table tt select null, null from temp_null;
select * from tt;

create table tt_b(a int, b string) row format serde "org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe";
insert overwrite table tt_b select null, null from temp_null;
select * from tt_b;

insert overwrite directory "../build/ql/test/data/warehouse/null_columns.out" select null, null from temp_null;
dfs -cat ../build/ql/test/data/warehouse/null_columns.out/*;

drop table tt;
drop table tt_b;
drop table temp_null;
