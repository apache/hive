SET hive.vectorized.execution.enabled=false;

-- The kv1 input file has 2 data fields, so when the 3 field struct is deserialized,
-- the premature end will put a NULL in field #3.
create table string_fields(strct struct<a:int, b:string, c:string>)
row format delimited
  fields terminated by '\t'
  collection items terminated by '\001';

load data local inpath '../../data/files/kv1.txt'
overwrite into table string_fields;

SELECT strct, strct.a, strct.b, strct.c FROM string_fields LIMIT 10;


create table char_fields(strct struct<a:int, b:char(10), c:char(10)>)
row format delimited
  fields terminated by '\t'
  collection items terminated by '\001';

load data local inpath '../../data/files/kv1.txt'
overwrite into table char_fields;

SELECT strct, strct.a, strct.b, strct.c FROM char_fields LIMIT 10;


create table varchar_fields(strct struct<a:int, b:varchar(5), c:varchar(5)>)
row format delimited
  fields terminated by '\t'
  collection items terminated by '\001';

load data local inpath '../../data/files/kv1.txt'
overwrite into table varchar_fields;

SELECT strct, strct.a, strct.b, strct.c FROM varchar_fields LIMIT 10;
