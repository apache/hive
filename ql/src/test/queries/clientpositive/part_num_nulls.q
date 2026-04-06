CREATE TABLE emp (eid INT, ename STRING) partitioned by (bdate INT, location STRING);

INSERT INTO emp
VALUES (1, 'Bob', 20200101, 'Paris'),
       (2, 'Alice', 20200102, 'Paris'),
       (3, 'Sam', 20200103, null),
       (4, 'John', null, 'New York'),
       (5, 'Jane', null, null),
       (6, 'Tom', null, 'New York'),
       (7, null, 20200103, 'New York'),
       (8, null, 20200103, 'Paris'),
       (null, 'Tom', 20200109, null),
       (null, 'Jane', 20200110, null);

DESCRIBE FORMATTED emp eid;
DESCRIBE FORMATTED emp ename;
DESCRIBE FORMATTED emp bdate;
DESCRIBE FORMATTED emp location;
