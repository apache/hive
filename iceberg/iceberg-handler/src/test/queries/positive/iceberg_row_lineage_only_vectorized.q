-- Vectorized Parquet reads that project row lineage columns with no or only some data columns
-- SORT_QUERY_RESULTS
set hive.fetch.task.conversion=none;

create table ice_t (id int, name string, balance int) stored by iceberg tblproperties ('format-version'='3');
insert into ice_t values (1,'aaa',25),(2,'bbb',35),(3,'ccc',82),(4,'ddd',91);
update ice_t set balance = 500 where id = 2;

create table ice_part (id int) partitioned by (p string) stored by iceberg tblproperties ('format-version'='3');
insert into ice_part values (1,'x'),(2,'x'),(3,'y');
update ice_part set id = 20 where id = 2;

set hive.vectorized.execution.enabled=true;

-- only row lineage columns
select ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t
where ROW__LINEAGE__ID = 1 or LAST__UPDATED__SEQUENCE__NUMBER = 2 order by ROW__LINEAGE__ID;
select ROW__LINEAGE__ID from ice_t;
select LAST__UPDATED__SEQUENCE__NUMBER, count(*) from ice_t group by LAST__UPDATED__SEQUENCE__NUMBER;

-- row lineage and data columns in non-schema order
select balance, ROW__LINEAGE__ID, id, LAST__UPDATED__SEQUENCE__NUMBER from ice_t;

-- identity partition column only: no data column is read from the file
select p, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_part;

set hive.vectorized.execution.enabled=false;

select ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t
where ROW__LINEAGE__ID = 1 or LAST__UPDATED__SEQUENCE__NUMBER = 2 order by ROW__LINEAGE__ID;
select ROW__LINEAGE__ID from ice_t;
select LAST__UPDATED__SEQUENCE__NUMBER, count(*) from ice_t group by LAST__UPDATED__SEQUENCE__NUMBER;
select balance, ROW__LINEAGE__ID, id, LAST__UPDATED__SEQUENCE__NUMBER from ice_t;
select p, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_part;

-- schema evolution: files written before and after adding and dropping columns
alter table ice_t add columns (score int);
insert into ice_t values (5,'eee',10,100);
alter table ice_t replace columns (id int, balance int, score int);
update ice_t set score = 200 where id = 1;

set hive.vectorized.execution.enabled=true;

select ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t;
select score, ROW__LINEAGE__ID, id from ice_t;

set hive.vectorized.execution.enabled=false;

select ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t;
select score, ROW__LINEAGE__ID, id from ice_t;
