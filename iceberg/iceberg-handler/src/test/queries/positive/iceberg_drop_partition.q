-- SORT_QUERY_RESULTS
create table ice_int (a int, b string) partitioned by spec (a) stored by iceberg;
insert into table ice_int values (1, 'ABC');
insert into table ice_int values (2, 'DEF');
insert into table ice_int values (3, 'ABC');
insert into table ice_int values (4, 'DEF');
insert into table ice_int values (1, 'ABC');
insert into table ice_int values (2, 'DEF');

alter table ice_int drop partition (a <= 2), partition (a >= 3);

select * from ice_int;

create table ice_str (a int, b string) partitioned by spec (b) stored by iceberg;
insert into table ice_str values (1, 'ABC');
insert into table ice_str values (2, 'DEF');
insert into table ice_str values (3, 'ABC');
insert into table ice_str values (4, 'DEF');
insert into table ice_str values (1, 'ABC');
insert into table ice_str values (2, 'XYZ');

alter table ice_str drop partition (b != 'ABC'), partition (b == 'XYZ');

select * from ice_str;

create table ice_int_double_part (a int, b int, c int) partitioned by spec (a, b) stored by iceberg;
insert into table ice_int_double_part values (1, 2, 3);
insert into table ice_int_double_part values (2, 3, 4);
insert into table ice_int_double_part values (3, 4, 5);
insert into table ice_int_double_part values (4, 5, 6);
insert into table ice_int_double_part values (1, 2, 4);
insert into table ice_int_double_part values (2, 1, 5);

alter table ice_int_double_part drop partition (a <= 2, b <= 1), partition (a >= 3, b != 4);

select * from ice_int_double_part;

create table ice_date_int_double_part (a date, b int, c int) partitioned by spec (a, b) stored by iceberg;
insert into table ice_date_int_double_part values ('2022-02-07', 2, 3);
insert into table ice_date_int_double_part values ('2022-08-07', 3, 4);
insert into table ice_date_int_double_part values ('2022-10-05', 4, 5);
insert into table ice_date_int_double_part values ('2022-01-17', 5, 6);
insert into table ice_date_int_double_part values ('2022-04-08', 2, 4);
insert into table ice_date_int_double_part values ('2023-02-07', 1, 5);

alter table ice_date_int_double_part drop partition (a <= '2023-02-07', b <= 1), partition (a >= '2022-08-07', b >= 2);

select * from ice_date_int_double_part;

create table ice_date_double_double_part (a date, b double, c int) partitioned by spec (a, b) stored by iceberg;
insert into table ice_date_double_double_part values ('2022-02-07', 2.75, 3);
insert into table ice_date_double_double_part values ('2022-08-07', 3.25, 4);
insert into table ice_date_double_double_part values ('2022-10-05', 4.23, 5);
insert into table ice_date_double_double_part values ('2022-01-17', 5.67, 6);
insert into table ice_date_double_double_part values ('2022-04-08', 2.45, 4);
insert into table ice_date_double_double_part values ('2023-02-07', 1.08, 5);

alter table ice_date_double_double_part drop partition (a <= '2023-02-07', b <= 1.09), partition (a >= '2022-08-07', b >= 2.78);

select * from ice_date_int_double_part;

create table ice_date_bigint_double_part (a date, b bigint, c int) partitioned by spec (a, b) stored by iceberg;
insert into table ice_date_bigint_double_part values ('2022-02-07', 267859937678997886, 3);
insert into table ice_date_bigint_double_part values ('2022-08-07', 325678599459970774, 4);
insert into table ice_date_bigint_double_part values ('2022-10-05', 423789504756478599, 5);
insert into table ice_date_bigint_double_part values ('2022-01-17', 567890387564883960, 6);
insert into table ice_date_bigint_double_part values ('2022-04-08', 245789600487678594, 4);
insert into table ice_date_bigint_double_part values ('2023-02-07', 108789600487566478, 5);

alter table ice_date_bigint_double_part drop partition (a <= '2023-02-07', b <= 109000000000000000L), partition (a >= '2022-08-07', b >= 278000000000000000L);

select * from ice_date_bigint_double_part;

drop table ice_int;
drop table ice_str;
drop table ice_int_double_part;
drop table ice_date_int_double_part;
drop table ice_date_double_double_part;
drop table ice_date_bigint_double_part;
