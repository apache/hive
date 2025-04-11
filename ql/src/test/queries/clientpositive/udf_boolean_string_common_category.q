create table boolarray1(id int, txt string, num int, flag string);
create table boolarray2(id int, txt string, num int, flag boolean);

insert into  boolarray1 values
  (1, 'one',   5, 'FALSE'),
  (2, 'two',  14, 'TRUE'),
  (3,  NULL,   3, 'FALSE');
insert into  boolarray2 values
  (1, 'one',   5, 'FALSE'),
  (2, 'two',  14, 'TRUE'),
  (3,  NULL,   3, 'FALSE');

select array(*) from boolarray1;
select array(*) from boolarray2;
