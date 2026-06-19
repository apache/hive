-- Checking autogathering column stats for DATE cols
set hive.stats.column.autogather=true;

create table acs_t11(a int,b int,d date);

-- should produce compute_stats aggregations
explain insert into acs_t11 values(1,1,'2011-11-11');

insert into acs_t11 values(1,1,'2011-11-11');
describe formatted acs_t11 d;

insert into acs_t11 values(1,1,NULL);
describe formatted acs_t11 d;

insert into acs_t11 values(1,1,'2006-11-11');
describe formatted acs_t11 d;

insert into acs_t11 values(1,1,'2001-11-11');
describe formatted acs_t11 d;

insert into acs_t11 values(1,1,'2004-11-11');
describe formatted acs_t11 d;

explain analyze table acs_t11 compute statistics for columns;
analyze table acs_t11 compute statistics for columns;

-- should be the same as before analyze
describe formatted acs_t11 d;
