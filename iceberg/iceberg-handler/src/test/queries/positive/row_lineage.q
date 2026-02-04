create table ice_t (id int, name string, balance int) stored by iceberg TBLPROPERTIES ('format-version'='3');
insert into ice_t values (1, 'aaa', 25),(2, 'bbb', 35),(3, 'ccc', 82),(4, 'ddd', 91);
select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t order by id;

update ice_t set balance = 500 where id = 2;

select id, name, balance, ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER from ice_t order by id;

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