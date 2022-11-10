set hive.query.results.cache.enabled=false;

-- create a test table
create table testhivemask(name string);

-- insert some values
insert into testhivemask values('name1'),('name2');

-- run the query with default values
select mask_hash(name) from testhivemask;

-- explicitily configure sha512 and check for values
set hive.masking.algo=sha512;
select mask_hash(name) from testhivemask;

-- explicitily configure sha256 and check for values
set hive.masking.algo=sha256;
select mask_hash(name) from testhivemask;