create temporary function udtfCount2 as 'org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFCount2';

set hive.fetch.task.conversion=minimal;
-- Correct output should be 2 rows
select udtfCount2() from src;

set hive.fetch.task.conversion=more;
-- Should still have the same output with fetch task conversion enabled
select udtfCount2() from src;

