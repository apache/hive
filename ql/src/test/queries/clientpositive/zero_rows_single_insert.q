-- Insert overwrite into when WHERE clause returns zero rows
DROP TABLE src_table;
CREATE TABLE src_table (key int);
LOAD DATA LOCAL INPATH '../../data/files/kv6.txt' INTO TABLE src_table;

DROP TABLE target_table;
CREATE TABLE target_table (key int);

SELECT COUNT(*) FROM target_table;
INSERT OVERWRITE TABLE target_table SELECT key FROM src_table;
SELECT COUNT(*) FROM target_table;
INSERT OVERWRITE TABLE target_table SELECT key FROM src_table WHERE FALSE;
SELECT COUNT(*) FROM target_table;
INSERT INTO TABLE target_table SELECT key FROM src_table;
SELECT COUNT(*) FROM target_table;
INSERT INTO TABLE target_table SELECT key FROM src_table WHERE FALSE;
SELECT COUNT(*) FROM target_table;
