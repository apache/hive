-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-hive-1.1.0-incubating-SNAPSHOT.jar;
-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/sketches-core-0.9.0.jar;
-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-java-1.2.0-incubating.jar;
-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-memory-1.2.0-incubating.jar


set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;


create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

-- build sketches per category
create table sketch_intermediate (category char(1), sketch binary)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
 
insert into sketch_intermediate select category, datatosketch(id) from sketch_input group by category;

-- create an mv for the intermediate results
create  materialized view mv_1 as
  select category, datatosketch(id) from sketch_input group by category;

-- see if we use the mv
explain
select category, SketchToEstimate(datatosketch(id)) from sketch_input group by category;

select category, SketchToEstimate(datatosketch(id)) from sketch_input group by category;

-- union sketches across categories and get overall unique count estimate
-- FIXME: this is currently not supported UDF(UDAF(UDAF(x)))
--explain
--select SketchToEstimate(unionSketch(datatosketch(id))) from sketch_input group by category;
--select SketchToEstimate(unionSketch(datatosketch(id))) from sketch_input group by category;

