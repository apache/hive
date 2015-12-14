CREATE TABLE src_null(a bigint) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/nulls.txt' INTO TABLE src_null;

create table all_nulls as SELECT a, cast(a as double) as b, cast(a as decimal) as c  FROM src_null where a is null limit 5;
analyze table all_nulls compute statistics for columns;

describe formatted all_nulls a;
describe formatted all_nulls b;

drop table all_nulls;
drop table src_null;
