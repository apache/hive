drop table if exists explain_npe_map;
drop table if exists explain_npe_array;
drop table if exists explain_npe_struct;

create table explain_npe_map    ( c1 map<string, string> );
create table explain_npe_array  ( c1 array<string> );
create table explain_npe_struct ( c1 struct<name:string, age:int> );

-- error
set hive.cbo.enable=false;
explain select c1 from explain_npe_map where c1 is null;
explain select c1 from explain_npe_array where c1 is null;
-- maybe correct in branch of master,
-- but i think it is necessary to initialize the value of StandardConstantStructObjectInspector
explain select c1 from explain_npe_struct where c1 is null;

-- correct
set hive.cbo.enable=true;
explain select c1 from explain_npe_map where c1 is null;
explain select c1 from explain_npe_array where c1 is null;
explain select c1 from explain_npe_struct where c1 is null;

-- drop test table
drop table if exists explain_npe_map;
drop table if exists explain_npe_array;
drop table if exists explain_npe_struct;