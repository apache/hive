create table t(i int, j int);
insert into t values(0,1), (0,2);

create table tt(i int, j int);
insert into tt values(0,3);

-- since this is correlated with COUNT aggregate and subquery returns 0 rows for group by (i=j) it should be a runtime error
select * from t where i NOT IN (select count(i) from tt where tt.j = t.j);

drop table t;
drop table tt;
