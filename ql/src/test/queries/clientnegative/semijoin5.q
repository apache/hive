create table t1 as select cast(key as int) key, value from src where key <= 10;

create table t5 as select cast(key as int) key1, value value1 from src where key <= 100;

-- reference rhs of semijoin in where-clause
explain select * from t1 left semi join t5 on value = value1 where key = 100 and key1 = 100;
