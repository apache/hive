-- SORT_QUERY_RESULTS

drop table if exists emptyavro;
create table emptyavro (a int) stored as avro;
select count(*) from emptyavro;
insert into emptyavro select count(*) from emptyavro;
select count(*) from emptyavro;
insert into emptyavro select key from src where key = 100 limit 1;
select * from emptyavro;

