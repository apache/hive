--! qt:dataset:src
create table load_overwrite_n0 like src;

insert overwrite table load_overwrite_n0 select * from src;
show table extended like load_overwrite_n0;
select count(*) from load_overwrite_n0;


load data local inpath '../../data/files/kv1.txt' into table load_overwrite_n0;
show table extended like load_overwrite_n0;
select count(*) from load_overwrite_n0;


load data local inpath '../../data/files/kv1.txt' overwrite into table load_overwrite_n0;
show table extended like load_overwrite_n0;
select count(*) from load_overwrite_n0;
