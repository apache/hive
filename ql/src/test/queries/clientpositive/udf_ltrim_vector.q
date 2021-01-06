create table t1 (col0 string);

insert into t1(col0) values ('xyfacebookyyx'),('   tech   ');

explain vectorization expression
select ltrim(col0) from t1 group by col0;

select ltrim(col0) from t1 group by col0;


explain vectorization expression
select ltrim(col0, 'xy') from t1 group by col0;

select ltrim(col0, 'xy') from t1 group by col0;

drop table t1;
