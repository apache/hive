set hive.query.results.cache.enabled=false;

-- create a test table
create table testhivemask(name string);

-- insert some values
insert into testhivemask values('name1'),('name2');

-- explicitily configure sha512 and check for values
set hive.fetch.task.conversion=none;
set hive.masking.algo=sha512;

-- try the udf on a table
select mask_hash(name) from testhivemask;

-- try the udf with a constant value
select mask_hash('01-28-2021');

-- explicitily configure sha256 and check for values

set hive.masking.algo=sha256;
select mask_hash(name) from testhivemask;

select mask_hash('01-28-2021');