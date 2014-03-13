
create table load_local (id INT);

load data local inpath '../../data/files/ext_test/' into table load_local;

select * from load_local;
