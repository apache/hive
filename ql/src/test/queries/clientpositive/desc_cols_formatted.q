CREATE TABLE tbl_t (id int, point STRUCT<x:INT, y:INT>);

DESCRIBE FORMATTED tbl_t;

DESCRIBE FORMATTED tbl_t id;
DESCRIBE FORMATTED tbl_t point;

DESCRIBE tbl_t id;
DESCRIBE tbl_t point;

CREATE TABLE tbl_part(id int, point STRUCT<x:INT, y:INT>) PARTITIONED BY (name string);

DESCRIBE FORMATTED tbl_part;

DESCRIBE FORMATTED tbl_part id;
DESCRIBE FORMATTED tbl_part point;

DESCRIBE tbl_part id;
DESCRIBE tbl_part point;