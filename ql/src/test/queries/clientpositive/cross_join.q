set hive.explain.user=false;
-- current
explain select src.key from src join src src2;
-- ansi cross join
explain select src.key from src cross join src src2;
-- appending condition is allowed
explain select src.key from src cross join src src2 on src.key=src2.key;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;
set hive.mapjoin.hybridgrace.hashtable=true;

explain select src.key from src join src src2;
explain select src.key from src cross join src src2;
explain select src.key from src cross join src src2 on src.key=src2.key;
