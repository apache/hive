create table table1(id int, txt string, num int, flag string);
create table table2(id int, txt string, num int, flag boolean);

insert into  table1 values
  (1, 'one',   5, 'FALSE'),
  (2, 'two',  14, 'TRUE'),
  (3,  NULL,   3, 'FALSE');
insert into  table2 values
  (1, 'one',   5, 'FALSE'),
  (2, 'two',  14, 'TRUE'),
  (3,  NULL,   3, 'FALSE');
  
select cast(array(*) as string) from table1;
select cast(array(*) as string) from table2;
select md5(s) csum from (select cast(array(*) as string) s from table1) t;
select md5(s) csum from (select cast(array(*) as string) s from table2) t;

select cast(struct(*) as string) from table1;
select cast(struct(*) as string) from table2;
select md5(s) csum from (select cast(struct(*) as string) s from table1) t;
select md5(s) csum from (select cast(struct(*) as string) s from table2) t;

select cast(map("id", id, "txt", txt, "num", num, "flag", flag) as string) from table1;
select cast(map("id", id, "txt", txt, "num", num, "flag", flag) as string) from table2;
select md5(s) csum from (select cast(map("id", id, "txt", txt, "num", num, "flag", flag) as string) s from table1) t;
select md5(s) csum from (select cast(map("id", id, "txt", txt, "num", num, "flag", flag) as string) s from table2) t;

select cast(create_union(if(id<=2, 0, 1), id, struct(txt, num, flag)) as string) from table1;
select cast(create_union(if(id<=2, 0, 1), id, struct(txt, num, flag)) as string) from table2;
select md5(s) csum from (select cast(create_union(if(id<=2, 0, 1), id, struct(txt, num, flag)) as string) s from table1) t;
select md5(s) csum from (select cast(create_union(if(id<=2, 0, 1), id, struct(txt, num, flag)) as string) s from table2) t;
