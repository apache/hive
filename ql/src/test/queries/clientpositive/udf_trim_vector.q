create table t1 (col0 string);

insert into t1(col0) values ('xyfacebookyyx'),('   tech   ');

explain vectorization expression
select trim(col0) from t1 group by col0;

select trim(col0) from t1 group by col0;


explain vectorization expression
select trim(col0, 'xy') from t1 group by col0;

select trim(col0, 'xy') from t1 group by col0;

drop table t1;
