

create table insert1(key int, value string) stored as textfile;
create table insert2(key int, value string) stored as textfile;
insert overwrite table insert1 select a.key, a.value from insert2 a WHERE (a.key=-1);


