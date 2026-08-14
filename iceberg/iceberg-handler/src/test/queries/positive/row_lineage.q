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

-- cow merge delete only
CREATE TABLE merge_source (
  id INT,
  data STRING
);

INSERT INTO merge_source VALUES
  (2, 'banana_source');

CREATE TABLE ice_cow_merge_delete_only (
  id INT,
  data STRING
)
STORED BY iceberg
TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='copy-on-write');

-- Snapshot 1: Sequence 1
INSERT INTO ice_cow_merge_delete_only VALUES
  (1, 'apple'),
  (2, 'banana'),
  (3, 'cherry');

SELECT id, data, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_cow_merge_delete_only
ORDER BY id;

MERGE INTO ice_cow_merge_delete_only t
USING merge_source s
ON t.id = s.id
WHEN MATCHED THEN DELETE;

-- Verification: id=1 and id=3 should perfectly retain their original lineage
SELECT id, data, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
FROM ice_cow_merge_delete_only
ORDER BY id;
