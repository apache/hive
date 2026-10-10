-- Negative: non-primitive partition column on CREATE VIEW (PARTITION_COLUMN_NON_PRIMITIVE)
create table pv_complex_src (id int, p struct<f:string>) stored as orc;
insert into pv_complex_src values (1, named_struct('f', 'x'));

create view pv_complex partitioned on (p) as select id, p from pv_complex_src;
