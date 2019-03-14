set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;

create temporary table foo(x int , y decimal(7,2));
create temporary table bar(x int , y decimal(7,2));
set hive.explain.user=false;
explain vectorization detail select sum(foo.y) from foo, bar where foo.x = bar.x;
