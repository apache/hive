drop table if exists tez_self_join1;
drop table if exists tez_self_join2;

create table tez_self_join1(id1 int, id2 string, id3 string);
insert into table tez_self_join1 values(1, 'aa','bb'), (2, 'ab','ab'), (3,'ba','ba');

create table tez_self_join2(id1 int);
insert into table tez_self_join2 values(1),(2),(3);

explain
select s.id2, s.id3
from  
(
 select self1.id1, self1.id2, self1.id3
 from tez_self_join1 self1 join tez_self_join1 self2
 on self1.id2=self2.id3 ) s
join tez_self_join2
on s.id1=tez_self_join2.id1
where s.id2='ab';

select s.id2, s.id3
from  
(
 select self1.id1, self1.id2, self1.id3
 from tez_self_join1 self1 join tez_self_join1 self2
 on self1.id2=self2.id3 ) s
join tez_self_join2
on s.id1=tez_self_join2.id1
where s.id2='ab';

drop table tez_self_join1;
drop table tez_self_join2;
