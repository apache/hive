--! qt:dataset:src
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.test.vectorizer.suppress.fatal.exceptions=false;

drop table if exists vector_and_or;
create table vector_and_or (dt1 date, dt2 date) stored as orc;

insert into table vector_and_or
  select null, null from src limit 1;
insert into table vector_and_or
  select date '1999-12-31', date '2000-01-01' from src limit 1;
insert into table vector_and_or
  select date '2001-01-01', date '2001-06-01' from src limit 1;

select '*** OR ***';
-- select null explicitly

explain vectorization detail select null or dt1 is not null from vector_and_or;
select '*** vectorized null or dt1 is not null ***';
select null or dt1 is not null from vector_and_or;

set hive.vectorized.execution.enabled=false;
select '*** non-vectorized null or dt1 is not null ***';
select null or dt1 is not null from vector_and_or;
set hive.vectorized.execution.enabled=true;


-- select boolean constant, already vectorized

explain vectorization detail select false or dt1 is not null from vector_and_or;
select '*** vectorized false or dt1 is not null ***';
select false or dt1 is not null from vector_and_or;

set hive.vectorized.execution.enabled=false;
select '*** non-vectorized false or dt1 is not null ***';
select false or dt1 is not null from vector_and_or;
set hive.vectorized.execution.enabled=true;

-- select dt1 = dt1 which is translated to "null or ..." after HIVE-21001

explain vectorization detail select dt1 = dt1 from vector_and_or;
select '*** vectorized dt1=dt1 ***';
select dt1 = dt1 from vector_and_or;

set hive.vectorized.execution.enabled=false;
select '*** non-vectorized dt1=dt1 ***';
select dt1 = dt1 from vector_and_or;
set hive.vectorized.execution.enabled=true;


select '*** AND ***';
-- select null explicitly

explain vectorization detail select null and dt1 is null from vector_and_or;
select '*** vectorized null and dt1 is null ***';
select null and dt1 is null from vector_and_or;

set hive.vectorized.execution.enabled=false;
select '*** non-vectorized null and dt1 is null ***';
select null and dt1 is null from vector_and_or;
set hive.vectorized.execution.enabled=true;


-- select boolean constant, already vectorized

explain vectorization detail select true and dt1 is null from vector_and_or;
select '*** vectorized true and dt1 is null ***';
select true and dt1 is null from vector_and_or;

set hive.vectorized.execution.enabled=false;
select '*** non-vectorized true and dt1 is null ***';
select true and dt1 is null from vector_and_or;
set hive.vectorized.execution.enabled=true;

-- select dt1 != dt1 which is translated to "null and ..." after HIVE-21001

explain vectorization detail select dt1 != dt1 from vector_and_or;
select '*** vectorized dt1!=dt1 ***';
select dt1 != dt1 from vector_and_or;

set hive.vectorized.execution.enabled=false;
select '*** non-vectorized dt1!=dt1 ***';
select dt1 != dt1 from vector_and_or;