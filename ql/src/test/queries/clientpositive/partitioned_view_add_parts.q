create table src_t (
  label string,
  p_int int,
  p_bool boolean,
  p_date date,
  p_dec decimal(10,2)
) stored as orc;

insert into src_t values
  ('row1', 42, true, '2024-06-01', 99.50);

create view vp_int partitioned on (p_int) as select label, p_int from src_t;
alter view vp_int add partition (p_int=42);
show partitions vp_int;

create view vp_bool partitioned on (p_bool) as select label, p_bool from src_t;
alter view vp_bool add partition (p_bool=true);
show partitions vp_bool;

create view vp_date partitioned on (p_date) as select label, p_date from src_t;
alter view vp_date add partition (p_date='2024-06-01');
show partitions vp_date;

create view vp_dec partitioned on (p_dec) as select label, p_dec from src_t;
alter view vp_dec add partition (p_dec=99.50);
show partitions vp_dec;

drop view vp_int;
drop view vp_bool;
drop view vp_date;
drop view vp_dec;
drop table src_t;
