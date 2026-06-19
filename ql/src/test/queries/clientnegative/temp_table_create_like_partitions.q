create table tab1 (c1 string) partitioned by (p1 string);
create table tmp1 like tab1;
show table create tmp1;
