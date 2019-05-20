create table if not exists numt --comment1
 (idx int); --comment2
insert into numt values(1); --comment3
insert into numt values(2);
--comment4
select idx from numt where --comment5
idx = 1; --comment6
select idx from numt where idx = 2 --comment6
limit 1;
--comment7
select "this is
another --string value" from numt where idx =2; --comment8
select 1, --comment
2;
drop table numt;
