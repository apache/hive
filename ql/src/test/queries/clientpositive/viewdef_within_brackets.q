create table tbl_x (a int, b string);
insert into tbl_x values (1, 'Prince'); insert into tbl_x values (2, 'John');
create view vw_x (b) as (select a from tbl_x);
select * from vw_x;

create view vw_y (col_b) as (
    ( ( select b from tbl_x ) )
    );
select * from vw_y;

create view vw_z as (
    ( ( select * from tbl_x )
     )
    );
select * from vw_z;