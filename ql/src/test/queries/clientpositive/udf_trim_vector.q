create table t1 (col0 string, col1 string);

insert into t1(col0, col1) values ('xyfacebookyyx', 'xy'),('   tech   ', ' ');

explain vectorization expression
select trim(col0) from t1 group by col0;

select trim(col0) from t1 group by col0;


explain vectorization expression
select trim(col0, 'xy') from t1 group by col0;

select trim(col0, 'xy') from t1 group by col0;


explain vectorization expression
select trim(col0, col1) from t1 group by col0, col1;

select trim(col0, col1) from t1 group by col0, col1;


explain vectorization expression
select trim('xyfacebookyyx', col1) from t1 group by col1;

select trim('xyfacebookyyx', col1) from t1 group by col1;


insert into t1(col0, col1) values (null, 'xy'),('foo', null);

select trim(col0, col1) from t1 where col0 is null;
select trim(col0, col1) from t1 where col1 is null;

select trim(col0, null) from t1 group by col0;
select trim(null, col1) from t1 group by col1;

drop table t1;
