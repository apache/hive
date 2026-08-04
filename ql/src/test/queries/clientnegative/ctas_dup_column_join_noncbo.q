set hive.cbo.enable=false;
create table cj3 (k int, v int);
create table cj4 (k int, w int);
create table ctas_dup_join_nc as select a.k, b.k from cj3 a join cj4 b on a.k = b.k;
