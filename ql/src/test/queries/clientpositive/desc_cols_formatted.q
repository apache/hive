CREATE TABLE tbl_t (id int, Point STRUCT<x:INT, y:INT>);

DESCRIBE FORMATTED tbl_t;

DESCRIBE FORMATTED tbl_t id;
DESCRIBE FORMATTED tbl_t Point;

DESCRIBE tbl_t id;
DESCRIBE tbl_t Point;

CREATE TABLE tbl_part(id int, Point STRUCT<x:INT, y:INT>) PARTITIONED BY (name string);

DESCRIBE FORMATTED tbl_part;

DESCRIBE FORMATTED tbl_part id;
DESCRIBE FORMATTED tbl_part Point;

DESCRIBE tbl_part id;
DESCRIBE tbl_part Point;