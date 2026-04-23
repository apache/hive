-- SORT_QUERY_RESULTS

create database ice_native_view_db;
use ice_native_view_db;

create table src_ice (
    first_name string, 
    last_name string
 )
partitioned by (dept_id bigint)
stored by iceberg stored as orc;

insert into src_ice values ('fn1','ln1', 1);
insert into src_ice values ('fn2','ln2', 1);
insert into src_ice values ('fn3','ln3', 1);
insert into src_ice values ('fn4','ln4', 1);
insert into src_ice values ('fn5','ln5', 2);
insert into src_ice values ('fn6','ln6', 2);
insert into src_ice values ('fn7','ln7', 2);

update src_ice set last_name = 'ln1a' where first_name='fn1';
update src_ice set last_name = 'ln2a' where first_name='fn2';
update src_ice set last_name = 'ln3a' where first_name='fn3';
update src_ice set last_name = 'ln4a' where first_name='fn4';
update src_ice set last_name = 'ln5a' where first_name='fn5';
update src_ice set last_name = 'ln6a' where first_name='fn6';
update src_ice set last_name = 'ln7a' where first_name='fn7';

delete from src_ice where last_name in ('ln1a', 'ln2a', 'ln7a');

create view v_ice as select * from src_ice stored by iceberg;

select * from v_ice;

drop view v_ice;
drop table src_ice;
use default;
drop database ice_native_view_db cascade;
