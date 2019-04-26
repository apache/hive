set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

create table tmask(t tinyint, s smallint, i int, bg bigint, f float, db double, dc decimal (10,3), n numeric,
	d date, ts timestamp,
	str string, vr varchar(10), ch char(4),
	b boolean, bin binary);

insert into tmask values(1,2,345,4455433,5.6,5644.455,10.20, 579.00, '2019-09-09', current_timestamp(), 'string1', 'varchar1', 'ch1', true, 'bin'),
		(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
		(9,7,3450,7455433,5.08,5944.455,10.20, 579.00, '1019-09-09', current_timestamp(), 'string2', 'varchar2', 'ch2', false, 'bin2'),
		(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);


-- MASK UDF with single argument
-- date types (timestamp is not supported for masking)
set hive.vectorized.execution.enabled=true;
explain VECTORIZATION DETAIL select mask(d), mask(ts) from tmask where s > 0 and i < 10000000;
select mask(d), mask(ts) from tmask where s > 0 and i < 10000000;
set hive.vectorized.execution.enabled=false;
select mask(d), mask(ts) from tmask where s > 0 and i < 10000000;

-- numeric types, double, float, demical and numeric are not supported for masking
set hive.vectorized.execution.enabled=true;
explain VECTORIZATION DETAIL select mask(t), mask(s), mask(i), mask(bg), mask(f), mask(db), mask(dc), mask(n) from tmask;
select mask(t), mask(s), mask(i), mask(bg), mask(f), mask(db), mask(dc), mask(n) from tmask;
set hive.vectorized.execution.enabled=false;
select mask(t), mask(s), mask(i), mask(bg), mask(f), mask(db), mask(dc), mask(n) from tmask;

-- string + misc types
set hive.vectorized.execution.enabled=true;
explain VECTORIZATION DETAIL select mask(str), mask(vr), mask(ch), mask(b), mask(bin) from tmask ;
select mask(str), mask(vr), mask(ch), mask(b), mask(bin) from tmask ;
set hive.vectorized.execution.enabled=false;
select mask(str), mask(vr), mask(ch), mask(b), mask(bin) from tmask ;

create temporary table tmask_temp(t date, s string, i string, bg string, f string, db string, dc string, n string,
	d string, ts string,
	str string, vr string, ch string,
	b string, bin string);
set hive.vectorized.execution.enabled=true;
insert into tmask_temp select mask(d), mask(ts), mask(t), mask(s), mask(i), mask(bg), mask(f), mask(db), mask(dc), mask(n),
    mask(str), mask(vr), mask(ch), mask(b), mask(bin) from tmask ;
select count(*) from tmask_temp group by (t,s,i,bg,f,db,dc,n,d,ts,str,vr,ch,b,bin);
set hive.vectorized.execution.enabled=false;
insert into tmask_temp select mask(d), mask(ts), mask(t), mask(s), mask(i), mask(bg), mask(f), mask(db), mask(dc), mask(n),
    mask(str), mask(vr), mask(ch), mask(b), mask(bin) from tmask ;

set hive.vectorized.execution.enabled=true;
-- should be double the above select count
select count(*) from tmask_temp group by (t,s,i,bg,f,db,dc,n,d,ts,str,vr,ch,b,bin);

DROP TABLE tmask;
