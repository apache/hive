set hive.cbo.enable=false;

CREATE TABLE T_A ( id STRING, val STRING ); 
CREATE TABLE T_B ( id STRING, val STRING ); 
CREATE TABLE join_result_1 ( ida STRING, vala STRING, idb STRING, valb STRING ); 
CREATE TABLE join_result_3 ( ida STRING, vala STRING, idb STRING, valb STRING ); 

INSERT INTO TABLE T_A 
VALUES ('Id_1', 'val_101'), ('Id_2', 'val_102'), ('Id_3', 'val_103'); 

INSERT INTO TABLE T_B 
VALUES ('Id_1', 'val_103'), ('Id_2', 'val_104'); 

explain
FROM T_A a LEFT JOIN T_B b ON a.id = b.id
INSERT OVERWRITE TABLE join_result_1
SELECT a.*, b.*
WHERE b.id = 'Id_1' AND b.val = 'val_103';

explain
FROM T_A a LEFT JOIN T_B b ON a.id = b.id
INSERT OVERWRITE TABLE join_result_3
SELECT a.*, b.*
WHERE b.val = 'val_104' AND b.id = 'Id_2' AND a.val <> b.val;

explain
FROM T_A a LEFT JOIN T_B b ON a.id = b.id
INSERT OVERWRITE TABLE join_result_1
SELECT a.*, b.*
WHERE b.id = 'Id_1' AND b.val = 'val_103'
INSERT OVERWRITE TABLE join_result_3
SELECT a.*, b.*
WHERE b.val = 'val_104' AND b.id = 'Id_2' AND a.val <> b.val;

explain
FROM T_A a LEFT JOIN T_B b ON a.id = b.id
INSERT OVERWRITE TABLE join_result_1
SELECT a.*, b.*
WHERE b.id = 'Id_1' AND b.val = 'val_103'
INSERT OVERWRITE TABLE join_result_3
SELECT a.*, b.*
WHERE b.val = 'val_104' AND b.id = 'Id_2';

explain
FROM T_A a JOIN T_B b ON a.id = b.id
INSERT OVERWRITE TABLE join_result_1
SELECT a.*, b.*
WHERE b.id = 'Id_1' AND b.val = 'val_103'
INSERT OVERWRITE TABLE join_result_3
SELECT a.*, b.*
WHERE b.val = 'val_104' AND b.id = 'Id_2';
