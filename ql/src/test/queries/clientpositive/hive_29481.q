set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- Test reproduction of HIVE-29481: NumberFormatException when querying insert_only tables with copy suffixes

-- Create a table with insert_only transactional property
CREATE TABLE hive_29481_io(id INT) STORED AS ORC 
LOCATION '${system:test.tmp.dir}/hive_29481_io'
TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only');

-- Insert some data to create a valid ACID delta directory
INSERT INTO hive_29481_io VALUES (1);

-- Show existing directories
dfs -ls -R ${system:test.tmp.dir}/hive_29481_io;

-- Manually create a directory with a _copy_ suffix to simulate the issue
-- AcidUtils.ParsedDeltaLight.parse will be called on any directory starting with delta_
-- We'll create it with writeId 2 (which is after the first insert's writeId 1)
dfs -mkdir ${system:test.tmp.dir}/hive_29481_io/delta_0000002_0000002_0000_copy_1;

-- Verify it was created
dfs -ls -R ${system:test.tmp.dir}/hive_29481_io;

-- This query should fail with NumberFormatException: For input string: "0000_copy_1"
SELECT * FROM hive_29481_io;

-- Clean up
DROP TABLE hive_29481_io;
