
create table t (a integer);

explain select not ((a>0) is not true) from t group by a;
explain select not ((a>0) is not false) from t group by a;
explain select not ((a>0) is not null) from t group by a;

explain select not ((a>0) is true) from t group by a;
explain select not ((a>0) is false) from t group by a;
explain select not ((a>0) is null) from t group by a;
