--! qt:dataset:src
-- SORT_QUERY_RESULTS

drop table if exists emptyavro_n0;
create table emptyavro_n0 (a int) stored as avro;
select count(*) from emptyavro_n0;
insert into emptyavro_n0 select count(*) from emptyavro_n0;
select count(*) from emptyavro_n0;
insert into emptyavro_n0 select key from src where key = 100 limit 1;
select * from emptyavro_n0;

