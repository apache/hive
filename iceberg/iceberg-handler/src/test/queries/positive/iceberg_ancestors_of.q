--! qt:replace:/[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]*)*(\s*)/#Timestamp#$2/
--! qt:replace:/([0-9]{17,19})/#SnapshotId#/

CREATE TABLE ice_anc_test (id INT, name STRING) STORED BY ICEBERG;

-- Create Snapshot 1
INSERT INTO ice_anc_test VALUES (1, 'A');
-- Create Snapshot 2
INSERT INTO ice_anc_test VALUES (2, 'B');
-- Create Snapshot 3
INSERT INTO ice_anc_test VALUES (3, 'C');
-- Create Snapshot 4
INSERT INTO ice_anc_test VALUES (4, 'D');

-- Test 1: Get Ancestors of current snapshot (Will output 4 snapshots)
SELECT iceberg_ancestors_of('default.ice_anc_test') AS (snapshot_id, ts);

-- Create a branch at the current snapshot
ALTER TABLE ice_anc_test CREATE BRANCH test_branch;

-- Insert into main to advance main (Snapshot 5)
INSERT INTO ice_anc_test VALUES (5, 'E');
-- Insert into main (Snapshot 6)
INSERT INTO ice_anc_test VALUES (6, 'F');

-- Insert into branch (Snapshot 7 - diverges from main)
INSERT INTO default.ice_anc_test.branch_test_branch VALUES (7, 'G');

-- Test 2: Get Ancestors from the branch
CREATE TEMPORARY TABLE branch_snap_id AS
SELECT snapshot_id FROM default.ice_anc_test.refs WHERE name = 'test_branch';

SELECT iceberg_ancestors_of('default.ice_anc_test', snapshot_id) FROM branch_snap_id;

-- Test 3: Validate ancestors actually exist in the table's snapshots metadata
SELECT count(*)
FROM (SELECT iceberg_ancestors_of('default.ice_anc_test') AS (snapshot_id, ts)) a
JOIN default.ice_anc_test.snapshots s
ON a.snapshot_id = s.snapshot_id;
