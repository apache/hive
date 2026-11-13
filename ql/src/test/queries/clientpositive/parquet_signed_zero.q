-- A batch of 0.0 and -0.0 is not a repeating batch: the two compare equal as doubles but are different values.
-- 2,000 rows alternating 0.0 and -0.0, DOUBLE and FLOAT, in PLAIN pages and in dictionary pages (two values),
-- read vectorized and row by row: 1,000 of each sign.
set hive.mapred.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;
set hive.fetch.task.conversion=none;

create table pz_src (d double, f float);
insert into pz_src values (cast('0.0' as double), cast('0.0' as float)), (cast('-0.0' as double), cast('-0.0' as float));
create table pz_plain (d double, f float) stored as parquet tblproperties ('parquet.enable.dictionary'='false');
create table pz_dict (d double, f float) stored as parquet tblproperties ('parquet.enable.dictionary'='true');
insert into pz_plain select s.d, s.f from (select posexplode(split(space(999), ' '))) t, pz_src s;
insert into pz_dict select s.d, s.f from (select posexplode(split(space(999), ' '))) t, pz_src s;

set hive.vectorized.execution.enabled=false;
select cast(d as string), cast(f as string), count(*) from pz_plain group by cast(d as string), cast(f as string);
select cast(d as string), cast(f as string), count(*) from pz_dict group by cast(d as string), cast(f as string);

set hive.vectorized.execution.enabled=true;
select cast(d as string), cast(f as string), count(*) from pz_plain group by cast(d as string), cast(f as string);
select cast(d as string), cast(f as string), count(*) from pz_dict group by cast(d as string), cast(f as string);
