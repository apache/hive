drop table if exists t;
create table t (a int,v int, b boolean);
insert into t values (1,null, true);
insert into t values (2,1,    false);
insert into t values (3,2,    null);

select assert_true(sum(a*a) = 1) from t
	where v is null;
select assert_true(sum(a*a) = 2*2+3*3) from t
	where v is not null;

select assert_true(sum(a*a) = 1) from t
	where b is true;

select assert_true(sum(a*a) = 2*2 + 3*3) from t
	where b is not true;

select assert_true(sum(a*a) = 4) from t
	where b is false;

select assert_true(sum(a*a) = 1*1 + 3*3) from t
	where b is not false;

select assert_true(sum(a*a) = 2*2) from t
	where (v>0 and v<2) is true;

select assert_true(sum(a*a) = 2*2) from t
	where (v<2) is true;

select  NULL is true,
        NULL is not true,
        NULL is false,
        NULL is not false
from t;
