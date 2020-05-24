create temporary table daysales_temp (customer int) partitioned by (dt string);

insert into daysales_temp partition(dt='2001-01-01') values(1);
insert into daysales_temp partition(dt='2001-01-03') values(3);

select * from daysales_temp where nvl(dt='2001-01-01' and customer=1, false);
select * from daysales_temp where nvl(dt='2001-01-02' and customer=1, false);
select * from daysales_temp where nvl(dt='2001-01-01' and customer=1, true);
select * from daysales_temp where (dt='2001-01-01' and customer=1);
select * from daysales_temp where (dt='2001-01-01' or customer=3);
select * from daysales_temp where (dt='2001-01-03' or customer=100);

explain extended select * from daysales_temp where nvl(dt='2001-01-01' and customer=1, false);
explain extended select * from daysales_temp where nvl(dt='2001-01-01' or customer=3, false);
explain extended select * from daysales_temp where nvl(dt='2001-01-01' or customer=3, false);
