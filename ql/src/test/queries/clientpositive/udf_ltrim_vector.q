create table t1 (col0 string, col1 string);

insert into t1(col0, col1) values ('xyfacebookyyx', 'xy'),('   tech   ', ' ');

explain vectorization expression
select ltrim(col0) from t1 group by col0;

select ltrim(col0) from t1 group by col0;


explain vectorization expression
select ltrim(col0, 'xy') from t1 group by col0;

select ltrim(col0, 'xy') from t1 group by col0;


explain vectorization expression
select ltrim(col0, col1) from t1 group by col0, col1;

select ltrim(col0, col1) from t1 group by col0, col1;


explain vectorization expression
select ltrim('xyfacebookyyx', col1) from t1 group by col1;

select ltrim('xyfacebookyyx', col1) from t1 group by col1;


insert into t1(col0, col1) values (null, 'xy'),('foo', null);

select ltrim(col0, col1) from t1 where col0 is null;
select ltrim(col0, col1) from t1 where col1 is null;

select ltrim(col0, null) from t1 group by col0;
select ltrim(null, col1) from t1 group by col1;

drop table t1;
