set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- 1. Test Insert-Only Table (Reproduce HIVE-29481)
CREATE TABLE hive_29481_io(id INT) STORED AS ORC 
LOCATION '${system:test.tmp.dir}/hive_29481_io'
TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only');

INSERT INTO hive_29481_io VALUES (1);
INSERT INTO hive_29481_io VALUES (4),(5),(6);

-- Manually create a directory with a _copy_ suffix (common during name collisions in moves)
-- AcidUtils used to fail with NumberFormatException on the suffix or ignore it if writeId was 0.
dfs -mkdir ${system:test.tmp.dir}/hive_29481_io/delta_0000000_0000000_copy_1;
-- Create a dummy file inside to ensure it's processed
dfs -touchz ${system:test.tmp.dir}/hive_29481_io/delta_0000000_0000000_copy_1/bucket_00000;

SELECT * FROM hive_29481_io;

-- 2. Test Full ACID Table (CRUD)
CREATE TABLE hive_29481_crud(id INT, val STRING) STORED AS ORC 
LOCATION '${system:test.tmp.dir}/hive_29481_crud'
TBLPROPERTIES('transactional'='true');

INSERT INTO hive_29481_crud VALUES (1, 'a'), (2, 'b');

-- Update a row (creates delta directory)
UPDATE hive_29481_crud SET val = 'updated' WHERE id = 1;

-- Delete a row (creates delete_delta directory)
DELETE FROM hive_29481_crud WHERE id = 2;

-- Simulate a _copy_ suffix on a delete_delta directory
dfs -mkdir ${system:test.tmp.dir}/hive_29481_crud/delete_delta_0000004_0000004_0000_copy_1;

SELECT * FROM hive_29481_crud;

-- Clean up
DROP TABLE hive_29481_io;
DROP TABLE hive_29481_crud;
