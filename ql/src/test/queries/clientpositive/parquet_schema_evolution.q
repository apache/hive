--! qt:dataset:srcpart

set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

-- Some tables might have extra columns and struct elements on the schema than the on Parquet schema;
-- This is called 'schema evolution' as the Parquet file is not ready yet for such new columns;
-- Hive should support this schema, and return NULL values instead;

DROP TABLE NewStructField;
DROP TABLE NewStructFieldTable;

CREATE TABLE NewStructField(a struct<a1:map<string,string>, a2:struct<e1:int>>) STORED AS PARQUET;

INSERT OVERWRITE TABLE NewStructField SELECT named_struct('a1', map('k1','v1'), 'a2', named_struct('e1',5)) FROM srcpart LIMIT 5;

DESCRIBE NewStructField;
SELECT * FROM NewStructField;
set hive.metastore.disallow.incompatible.col.type.changes=false;
-- Adds new fields to the struct types
ALTER TABLE NewStructField REPLACE COLUMNS (a struct<a1:map<string,string>, a2:struct<e1:int,e2:string>, a3:int>, b int);
reset hive.metastore.disallow.incompatible.col.type.changes;
DESCRIBE NewStructField;
SELECT * FROM NewStructField;

-- Makes sure that new parquet tables contain the new struct field
CREATE TABLE NewStructFieldTable STORED AS PARQUET AS SELECT * FROM NewStructField;
DESCRIBE NewStructFieldTable;
SELECT * FROM NewStructFieldTable;

-- test if the order of fields in array<struct<>> changes, it works fine

DROP TABLE IF EXISTS schema_test;
CREATE TABLE schema_test (msg array<struct<f1: string, f2: string, a: array<struct<a1: string, a2: string>>, b: array<struct<b1: int, b2: int>>>>) STORED AS PARQUET;
INSERT INTO TABLE schema_test SELECT array(named_struct('f1', 'abc', 'f2', 'abc2', 'a', array(named_struct('a1', 'a1', 'a2', 'a2')),
   'b', array(named_struct('b1', 1, 'b2', 2)))) FROM NewStructField LIMIT 2;
SELECT * FROM schema_test;
set hive.metastore.disallow.incompatible.col.type.changes=false;
-- Order of fields swapped
ALTER TABLE schema_test CHANGE msg msg array<struct<a: array<struct<a2: string, a1: string>>, b: array<struct<b2: int, b1: int>>, f2: string, f1: string>>;
reset hive.metastore.disallow.incompatible.col.type.changes;
SELECT * FROM schema_test;

DROP TABLE schema_test;
DROP TABLE NewStructField;
DROP TABLE NewStructFieldTable;

drop table if exists parq_test;
create table parq_test(age int, name string) stored as parquet;
insert into parq_test values(1, 'aaaa');

DESCRIBE parq_test;
alter table parq_test change age age string;
DESCRIBE parq_test;

insert into parq_test values('b', 'bbbb');

select * from parq_test;
select * from parq_test where age='b';
select * from parq_test where age='1';
select * from parq_test where age=1;

explain select * from parq_test where age='b';
explain select * from parq_test where age='1';
explain select * from parq_test where age=1;

explain vectorization expression select * from parq_test where age='b';
explain vectorization expression select * from parq_test where age='1';
explain vectorization expression select * from parq_test where age=1;

drop table parq_test;
