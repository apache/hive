CREATE TABLE merge_source (id INT, data STRING);
INSERT INTO merge_source VALUES (2, 'banana_source');

CREATE TABLE ice_cow_merge_delete_only (id INT, data STRING)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='copy-on-write');

INSERT INTO ice_cow_merge_delete_only VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry');

SELECT id, data FROM ice_cow_merge_delete_only ORDER BY id;

MERGE INTO ice_cow_merge_delete_only t
USING merge_source s
ON t.id = s.id
WHEN MATCHED THEN DELETE;

SELECT id, data FROM ice_cow_merge_delete_only ORDER BY id;
