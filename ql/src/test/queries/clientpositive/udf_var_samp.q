DESCRIBE FUNCTION var_samp;
DESCRIBE FUNCTION EXTENDED var_samp;
DESCRIBE FUNCTION var_samp;
DESCRIBE FUNCTION EXTENDED var_samp;

drop table if exists t;
create table t (a int);
insert into t values (1),(-1),(0);

select var_samp(a) from t;
select var_samp(a) from t where a=0;
select round(var_samp(a),5) from t where a>=0;

