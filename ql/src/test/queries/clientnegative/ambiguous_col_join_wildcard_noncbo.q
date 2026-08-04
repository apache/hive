set hive.cbo.enable=false;
create table wj3 (k int, v int);
create table wj4 (k int, w int);
select t.v from (select a.*, b.* from wj3 a join wj4 b on a.k = b.k) t;
