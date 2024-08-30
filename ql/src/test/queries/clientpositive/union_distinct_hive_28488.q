set hive.optimize.shared.work=false;

create table union_test (key string, value string);

set hive.optimize.merge.adjacent.union.distinct=false;
explain
select * from (
  select * from union_test
  union
  select * from union_test
  union
  select * from union_test
) d; 

set hive.optimize.merge.adjacent.union.distinct=true;
explain
select * from (
  select * from union_test
  union
  select * from union_test
  union
  select * from union_test
) d; 

