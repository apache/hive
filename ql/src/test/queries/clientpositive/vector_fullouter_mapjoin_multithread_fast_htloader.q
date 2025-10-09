set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=100000000000;
set hive.exec.reducers.max=1;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;
set hive.mapjoin.hashtable.load.threads=2;

-- SORT_QUERY_RESULTS

--
-- test Long key
--

-- This table should be broadcasted and stored in HashTable.
create table small_long_table (key bigint, value string);
insert into small_long_table values (7610878409923211200, "a"); -- key hash % 2097152 == 0
insert into small_long_table values (-371494529663898262, "b"); -- key hash % 2097152 == 0
insert into small_long_table values (-2307888158465848362, "c"); -- key hash % 2097152 == 1

create table big_long_table (key bigint, value string);
insert into big_long_table values (-2307888158465848362, "c"); -- key hash % 2097152 == 1

-- small table size should be larger than VectorMapJoinFastHashTable.FIRST_SIZE_UP.
-- If not, only a single thread loads entire hash table.
alter table big_long_table update statistics set   ('numRows'='90000000'); -- should be larger than small table
alter table small_long_table update statistics set ('numRows'='2097152'); -- 2 * VectorMapJoinFastHashTable.FIRST_SIZE_UP

-- query plan must includes vectorized fullouter mapjoin.
explain
select * from small_long_table full outer join big_long_table on (small_long_table.key = big_long_table.key);

select * from small_long_table full outer join big_long_table on (small_long_table.key = big_long_table.key);

--
-- test String key
--

-- This table should be broadcasted and stored in HashTable.
create table small_string_table (key string, value string);
insert into small_string_table values ("affzk", "a"); -- key hash % 2097152 == 0
insert into small_string_table values ("hbkpa", "b"); -- key hash % 2097152 == 0
insert into small_string_table values ("kykzm", "c"); -- key hash % 2097152 == 1

create table big_string_table (key string, value string);
insert into big_string_table values ("kykzm", "c"); -- key hash % 2097152 == 1

-- small table size should be larger than VectorMapJoinFastHashTable.FIRST_SIZE_UP.
-- If not, only a single thread loads entire hash table.
alter table big_string_table update statistics set   ('numRows'='90000000'); -- should be larger than small table
alter table small_string_table update statistics set ('numRows'='2097152'); -- 2 * VectorMapJoinFastHashTable.FIRST_SIZE_UP

-- query plan must includes vectorized fullouter mapjoin.
explain
select * from small_string_table full outer join big_string_table on (small_string_table.key = big_string_table.key);

select * from small_string_table full outer join big_string_table on (small_string_table.key = big_string_table.key);

-- To test multikey HashTable, one may use the following configuration.
-- set hive.vectorized.execution.mapjoin.native.multikey.only.enabled=true;

