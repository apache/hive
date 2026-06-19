--! qt:dataset:src

CREATE EXTERNAL TABLE hbase_pushdown(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#binary,cf:string")
TBLPROPERTIES ("external.table.purge" = "true");

INSERT OVERWRITE TABLE hbase_pushdown SELECT * FROM src;
INSERT INTO hbase_pushdown VALUES(-10, 'NegativeValue10'),(-9, 'NegativeValue9'),(-5, 'NegativeValue5'),(-2, 'NegativeValue2');

explain select * from hbase_pushdown where key >= 90 and key <= 100;
select * from hbase_pushdown where key >= 90 and key <= 100;

-- In HBase, Keys are sorted lexicographically so the byte representation of negative values comes after the positive values
-- So don't pushdown the predicate if any constant value is a negative number.
explain select * from hbase_pushdown where key >= -5 and key <= 5;
select * from hbase_pushdown where key >= -5 and key <= 5;

-- Here the predicate can be pushed down even though value is negative as its not a range predicate.
explain select * from hbase_pushdown where key = -5;
select * from hbase_pushdown where key = -5;


set hive.optimize.ppd.storage=false;

-- Now 'BETWEEN' predicate will not convert to '<= AND >=', also 'BETWEEN' predicate will not be pushed down
explain select * from hbase_pushdown where key >= 90 and key <= 100;
select * from hbase_pushdown where key >= 90 and key <= 100;