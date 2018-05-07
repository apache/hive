-- partition columns aren't allowed to have not null or default constraints
create table tpart(i int default 5, j int not null enable) partitioned by (s string not null);
