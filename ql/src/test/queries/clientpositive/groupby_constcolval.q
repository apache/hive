DROP TABLE IF EXISTS tlbtest;
create table tlbtest (key int, key1 int, key2 int);
select key, key1, key2 from (select a.key, 0 as key1 , 0 as key2 from tlbtest a inner join src b on a.key = b.key) a group by key, key1, key2;
select key, key1, key2 from (select a.key, 0 as key1 , 1 as key2 from tlbtest a inner join src b on a.key = b.key) a group by key, key1, key2;
