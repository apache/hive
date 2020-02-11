--! qt:dataset:src
explain select key || value from src;

select concat('a','b','c');
select 'a' || 'b' || 'c';

select '1' || 2+3;
select 1+2 || '7';

select 1 || 1 || 1;
select 1.2 || 1.7;
select 1 + 1 || 1 + 1;
select 9 + 9 || 9 + 9;
select 1 + 1 || 1 + 1 || 1 + 1;

-- || has higher precedence than bitwise ops...so () is neccessary
select '1' || 4 / 2 || 1 + 2 * 1 || (6 & 4) || (1 | 4);

-- however ^ is different from the other bitwise ops:
select 0 ^ 1 || '2' || 1 ^ 2;

create table ct1 (c int);
create table ct2 (c int);

insert into ct1 values (7),(5),(3),(1);
insert into ct2 values (8),(6),(4),(2);

create view ct_v1 as select * from ct1 union all select * from ct2;

select c,c * c + c || 'x', 'c+c=' || c+c || ', c*c=' || c*c || ', (c&c)=' || (c & c) from ct_v1 order by c;


select *, 'x' || (c&3) , 'a' || c*c+c || 'b' from ct_v1
		order by 'a' || c*c+c || 'b';

select 'x' || (c&3) from ct_v1
		group by 'x' || (c&3) order by 'x' || (c&3);

explain select concat('a','b','c');
explain select 'a' || 'b' || 'c';

-- check and/or precedence relation; should be true
-- (true and false) or (false and true) or true => true		psql/mysql/ora/hive
-- true and (false or false) and (true or true) => false	should not happen
select true and false or false and true or true;

explain formatted select key || value from src;

explain formatted select key || value || key from src;

explain formatted select key || value || key || value from src;
