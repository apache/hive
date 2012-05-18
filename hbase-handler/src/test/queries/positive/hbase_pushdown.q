CREATE TABLE hbase_pushdown(key int, value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string");

INSERT OVERWRITE TABLE hbase_pushdown 
SELECT *
FROM src;

-- with full pushdown
explain select * from hbase_pushdown where key=90;

select * from hbase_pushdown where key=90;

-- with partial pushdown

explain select * from hbase_pushdown where key=90 and value like '%90%';

select * from hbase_pushdown where key=90 and value like '%90%';

-- with two residuals

explain select * from hbase_pushdown
where key=90 and value like '%90%' and key=cast(value as int);

-- with contradictory pushdowns

explain select * from hbase_pushdown
where key=80 and key=90 and value like '%90%';

select * from hbase_pushdown
where key=80 and key=90 and value like '%90%';

-- with nothing to push down

explain select * from hbase_pushdown;

-- with a predicate which is not actually part of the filter, so
-- it should be ignored by pushdown

explain select * from hbase_pushdown
where (case when key=90 then 2 else 4 end) > 3;

-- with a predicate which is under an OR, so it should
-- be ignored by pushdown

explain select * from hbase_pushdown
where key=80 or value like '%90%';

set hive.optimize.ppd.storage=false;

-- with pushdown disabled

explain select * from hbase_pushdown where key=90;
