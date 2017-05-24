create table daysales (customer int) partitioned by (dt string);

insert into daysales partition(dt='2001-01-01') values(1);
insert into daysales partition(dt='2001-01-03') values(3);

select * from daysales where nvl(dt='2001-01-01' and customer=1, false);
select * from daysales where nvl(dt='2001-01-02' and customer=1, false);
select * from daysales where nvl(dt='2001-01-01' and customer=1, true);
select * from daysales where (dt='2001-01-01' and customer=1);
select * from daysales where (dt='2001-01-01' or customer=3);
select * from daysales where (dt='2001-01-03' or customer=100);

explain extended select * from daysales where nvl(dt='2001-01-01' and customer=1, false);
explain extended select * from daysales where nvl(dt='2001-01-01' or customer=3, false);
explain extended select * from daysales where nvl(dt='2001-01-01' or customer=3, false);
