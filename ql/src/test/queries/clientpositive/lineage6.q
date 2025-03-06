set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;
set hive.lineage.statement.filter=Query;

create table table_1_qegkz (id int, first_name string);
create table table_2_gkvuw (id int, last_name string);

select table_1_qegkz.id, concat_ws(' ' , table_1_qegkz.first_name, table_2_gkvuw.last_name) full_name from table_1_qegkz, table_2_gkvuw where table_1_qegkz.id = table_2_gkvuw.id;

create view view_fcuyp as (select table_1_qegkz.id, concat_ws(' ' , table_1_qegkz.first_name, table_2_gkvuw.last_name) full_name from table_1_qegkz, table_2_gkvuw where table_1_qegkz.id = table_2_gkvuw.id);

set hive.lineage.statement.filter=Query,CREATE_VIEW;

create view view_fcuyp2 as (select table_1_qegkz.id, concat_ws(' ' , table_1_qegkz.first_name, table_2_gkvuw.last_name) full_name from table_1_qegkz, table_2_gkvuw where table_1_qegkz.id = table_2_gkvuw.id);
