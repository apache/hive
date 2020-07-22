create table union_first_part (a int, b int);
insert into union_first_part values (1,2),(3,1);

create table union_second_part (a int, b int);
insert into union_second_part values (4,1),(2,3),(-1,2);

-- Testing insert with Union
create table union_all (a int, b int);
insert overwrite table union_all
    select * from union_first_part
union all
    select * from union_second_part;

select * from union_all order by a;