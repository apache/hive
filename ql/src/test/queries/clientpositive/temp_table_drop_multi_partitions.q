create temporary table mp_n0_temp (a string) partitioned by (b string, c string);

alter table mp_n0_temp add partition (b='1', c='1');
alter table mp_n0_temp add partition (b='1', c='2');
alter table mp_n0_temp add partition (b='2', c='2');

show partitions mp_n0_temp;

--explain extended alter table dmp.mp_n0_temp drop partition (b='1');
alter table mp_n0_temp drop partition (b='1');

show partitions mp_n0_temp;

drop table mp_n0_temp;
