create table t(i int, j int);
insert into t values(0,1), (0,2);

create table tt(i int, j int);
insert into tt values(0,3);

select * from t where i > ANY (select count(i) from tt where tt.j = t.j);

drop table t;
drop table tt;
