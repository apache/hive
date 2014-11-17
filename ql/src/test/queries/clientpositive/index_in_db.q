set hive.optimize.index.filter=true; 
drop database if exists index_test_db cascade;
-- Test selecting selecting from a table that is backed by an index
-- create table, index in a db, then set default db as current db, and try selecting

create database index_test_db;

use index_test_db;
create table testtb (id int, name string);
create index id_index on table testtb (id) as 'COMPACT' WITH DEFERRED REBUILD  in table testdb_id_idx_tb;

use default;
select * from index_test_db.testtb where id>2;
