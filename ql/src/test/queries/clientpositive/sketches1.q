-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-hive-1.1.0-incubating-SNAPSHOT.jar;
-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/sketches-core-0.9.0.jar;
-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-java-1.2.0-incubating.jar;
-- add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-memory-1.2.0-incubating.jar

create temporary table sketch_input (id int, category char(1));
insert into table sketch_input values
  (1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b');

-- build sketches per category
create table sketch_intermediate (category char(1), sketch binary);
insert into sketch_intermediate select category, datatosketch(id) from sketch_input group by category;

-- get unique count estimates per category
select category, SketchToEstimate(sketch) from sketch_intermediate;

-- union sketches across categories and get overall unique count estimate
select estimate(unionSketch(sketch)) from sketch_intermediate;

