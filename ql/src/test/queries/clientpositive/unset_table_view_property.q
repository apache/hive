--! qt:dataset:src
CREATE DATABASE vt;

CREATE TABLE vt.testTable(col1 INT, col2 INT);
SHOW TBLPROPERTIES vt.testTable;

-- UNSET TABLE PROPERTIES
ALTER TABLE vt.testTable SET TBLPROPERTIES ('a'='1', 'c'='3');
SHOW TBLPROPERTIES vt.testTable;

-- UNSET all the properties
ALTER TABLE vt.testTable UNSET TBLPROPERTIES ('a', 'c');
SHOW TBLPROPERTIES vt.testTable;

ALTER TABLE vt.testTable SET TBLPROPERTIES ('a'='1', 'c'='3', 'd'='4');
SHOW TBLPROPERTIES vt.testTable;

-- UNSET a subset of the properties
ALTER TABLE vt.testTable UNSET TBLPROPERTIES ('a', 'd');
SHOW TBLPROPERTIES vt.testTable;

-- the same property being UNSET multiple times
ALTER TABLE vt.testTable UNSET TBLPROPERTIES ('c', 'c', 'c');
SHOW TBLPROPERTIES vt.testTable;

ALTER TABLE vt.testTable SET TBLPROPERTIES ('a'='1', 'b' = '2', 'c'='3', 'd'='4');
SHOW TBLPROPERTIES vt.testTable;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER TABLE vt.testTable UNSET TBLPROPERTIES IF EXISTS ('b', 'd', 'b', 'f');
SHOW TBLPROPERTIES vt.testTable;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER TABLE vt.testTable UNSET TBLPROPERTIES IF EXISTS ('b', 'd', 'c', 'f', 'x', 'y', 'z');
SHOW TBLPROPERTIES vt.testTable;

DROP TABLE vt.testTable;

-- UNSET VIEW PROPERTIES
CREATE VIEW vt.testView AS SELECT value FROM src WHERE key=86;
ALTER VIEW vt.testView SET TBLPROPERTIES ('propA'='100', 'propB'='200');
SHOW TBLPROPERTIES vt.testView;

-- UNSET all the properties
ALTER VIEW vt.testView UNSET TBLPROPERTIES ('propA', 'propB');
SHOW TBLPROPERTIES vt.testView;

ALTER VIEW vt.testView SET TBLPROPERTIES ('propA'='100', 'propC'='300', 'propD'='400');
SHOW TBLPROPERTIES vt.testView;

-- UNSET a subset of the properties
ALTER VIEW vt.testView UNSET TBLPROPERTIES ('propA', 'propC');
SHOW TBLPROPERTIES vt.testView;

-- the same property being UNSET multiple times
ALTER VIEW vt.testView UNSET TBLPROPERTIES ('propD', 'propD', 'propD');
SHOW TBLPROPERTIES vt.testView;

ALTER VIEW vt.testView SET TBLPROPERTIES ('propA'='100', 'propB' = '200', 'propC'='300', 'propD'='400');
SHOW TBLPROPERTIES vt.testView;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER VIEW vt.testView UNSET TBLPROPERTIES IF EXISTS ('propC', 'propD', 'propD', 'propC', 'propZ');
SHOW TBLPROPERTIES vt.testView;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER VIEW vt.testView UNSET TBLPROPERTIES IF EXISTS ('propB', 'propC', 'propD', 'propF');
SHOW TBLPROPERTIES vt.testView;

DROP VIEW vt.testView;

DROP DATABASE vt;