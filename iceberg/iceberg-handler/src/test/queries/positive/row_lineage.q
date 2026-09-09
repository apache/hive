create table ice_t (id int, name string, balance int) stored by iceberg TBLPROPERTIES ('format-version'='3');
insert into ice_t values (1, 'aaa', 25),(2, 'bbb', 35),(3, 'ccc', 82),(4, 'ddd', 91);
select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t order by id;

update ice_t set balance = 500 where id = 2;

select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t order by id;

-- Test filtering with row lineage columns
select id, name, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t where ROW__LINEAGE__ID = 1;
select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t where LAST__UPDATED__SEQUENCE__NUMBER = 1;
select *, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t where LAST__UPDATED__SEQUENCE__NUMBER = 2 OR ROW__LINEAGE__ID = 1;
delete from ice_t where ROW__LINEAGE__ID = 1 OR LAST__UPDATED__SEQUENCE__NUMBER = 2;
select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t order by id;

-- copy-on-write
create table ice_t_cow (id int, name string, balance int) stored by iceberg TBLPROPERTIES ('format-version'='3', 'write.update.mode'='copy-on-write');
insert into ice_t_cow values (1, 'aaa', 25),(2, 'bbb', 35),(3, 'ccc', 82),(4, 'ddd', 91);
select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t_cow order by id;

update ice_t_cow set balance = 500 where id = 2;
select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t_cow order by id;

-- merge
CREATE TABLE ice_merge (
  id INT,
  data STRING
)
STORED BY iceberg
TBLPROPERTIES ('format-version'='3');

INSERT INTO ice_merge VALUES
  (1, 'a'),
  (2, 'b'),
  (3, 'c');

CREATE TABLE src (
  id INT,
  data STRING
)
STORED AS TEXTFILE;

INSERT INTO src VALUES
  (2, 'bb'),
  (4, 'd');

MERGE INTO ice_merge t
USING src s
ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET data = s.data
WHEN NOT MATCHED THEN
  INSERT VALUES (s.id, s.data);

SELECT id, data, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_merge
ORDER BY ROW__LINEAGE__ID;

-- merge cow
CREATE TABLE ice_merge_cow (
  id INT,
  data STRING
)
STORED BY iceberg
TBLPROPERTIES ('format-version'='3', 'write.merge.mode'='copy-on-write');

INSERT INTO ice_merge_cow VALUES
  (1, 'a'),
  (2, 'b'),
  (3, 'c');

MERGE INTO ice_merge_cow t
USING src s
ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET data = concat(s.data, '_changed');

SELECT id, data, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_merge_cow
ORDER BY ROW__LINEAGE__ID;

-- cow delete
CREATE TABLE ice_cow_delete (
  id INT,
  data STRING
)
STORED BY iceberg
TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='copy-on-write');

INSERT INTO ice_cow_delete VALUES
  (1, 'apple'),
  (2, 'banana'),
  (3, 'cherry');

SELECT id, data, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_cow_delete
ORDER BY id;

DELETE FROM ice_cow_delete WHERE id = 2;

SELECT id, data, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_cow_delete
ORDER BY id;

-- cow delete partitioned
CREATE TABLE ice_cow_delete_part (
  id INT,
  data STRING
)
PARTITIONED BY (part STRING)
STORED BY iceberg
TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='copy-on-write');

-- Snapshot 1: Sequence 1
INSERT INTO ice_cow_delete_part VALUES
  (1, 'apple', 'p1'),
  (2, 'banana', 'p1'),
  (3, 'cherry', 'p2'),
  (4, 'date', 'p2');

-- Snapshot 2: Sequence 2 (Adding more data to partition p1 to mix sequence numbers)
INSERT INTO ice_cow_delete_part VALUES
  (5, 'elderberry', 'p1');

SELECT id, data, part, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_cow_delete_part
ORDER BY id;

-- Snapshot 3: Delete across partitions (Affects data files in both p1 and p2)
DELETE FROM ice_cow_delete_part WHERE id = 2 OR id = 3;

-- id=1 and id=4 should retain Sequence 1
-- id=5 should retain Sequence 2
SELECT id, data, part, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_cow_delete_part
ORDER BY id;
