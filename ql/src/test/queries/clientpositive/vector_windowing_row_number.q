set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table row_number_test;

create table row_number_test as select explode(split(repeat("w,", 2400), ","));

insert into row_number_test select explode(split(repeat("x,", 1200), ","));

insert into row_number_test select explode(split(repeat("y,", 700), ","));

insert into row_number_test select explode(split(repeat("z,", 600), ","));

explain select
        row_number() over() as r1,
        row_number() over(order by col) r2,
        row_number() over(partition by col) r3,
        row_number() over(partition by col order by col) r4,
        row_number() over(partition by 1 order by col) r5,
        row_number() over(partition by col order by 2) r6,
        row_number() over(partition by 1 order by 2) r7,
        col
        from row_number_test;

create table row_numbers_vectorized as select
row_number() over() as r1,
row_number() over(order by col) r2,
row_number() over(partition by col) r3,
row_number() over(partition by col order by col) r4,
row_number() over(partition by 1 order by col) r5,
row_number() over(partition by col order by 2) r6,
row_number() over(partition by 1 order by 2) r7,
col
from row_number_test;

SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.ptf.enabled=false;

explain select
        row_number() over() as r1,
        row_number() over(order by col) r2,
        row_number() over(partition by col) r3,
        row_number() over(partition by col order by col) r4,
        row_number() over(partition by 1 order by col) r5,
        row_number() over(partition by col order by 2) r6,
        row_number() over(partition by 1 order by 2) r7,
        col
        from row_number_test;

create table row_numbers_non_vectorized as select
row_number() over() as r1,
row_number() over(order by col) r2,
row_number() over(partition by col) r3,
row_number() over(partition by col order by col) r4,
row_number() over(partition by 1 order by col) r5,
row_number() over(partition by col order by 2) r6,
row_number() over(partition by 1 order by 2) r7,
col
from row_number_test;

-- compare results of vectorized with those of non-vectorized execution

select exists(
select r1, r2, r3, r4, r5, r6, r7, col from row_numbers_vectorized
minus
select r1, r2, r3, r4, r5, r6, r7, col from row_numbers_non_vectorized
) diff_exists;

drop table row_numbers_non_vectorized;
drop table row_numbers_vectorized;
drop table row_number_test;
