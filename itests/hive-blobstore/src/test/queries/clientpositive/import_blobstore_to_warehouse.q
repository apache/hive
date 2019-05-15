-- Check we can create a partitioned table in the warehouse, 
-- export it to a blobstore, and then import the
-- whole table using the warehouse as target location
DROP TABLE exim_employee;
CREATE TABLE exim_employee (emp_id int COMMENT "employee id")
COMMENT "employee table"
PARTITIONED BY (emp_country string COMMENT "two char iso code")
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/test.dat"
INTO TABLE exim_employee PARTITION (emp_country="in");
LOAD DATA LOCAL INPATH "../../data/files/test.dat"
INTO TABLE exim_employee PARTITION (emp_country="us");
LOAD DATA LOCAL INPATH "../../data/files/test.dat"
INTO TABLE exim_employee PARTITION (emp_country="cz");

DESCRIBE EXTENDED exim_employee;
SELECT * FROM exim_employee;

dfs -rm -r -f ${hiveconf:test.blobstore.path.unique}/import_blobstore_to_warehouse/export/exim_employee;
EXPORT TABLE exim_employee PARTITION (emp_country='us')
TO '${hiveconf:test.blobstore.path.unique}/import_blobstore_to_warehouse/export/exim_employee';

DROP TABLE exim_employee;
IMPORT FROM '${hiveconf:test.blobstore.path.unique}/import_blobstore_to_warehouse/export/exim_employee';

DESCRIBE EXTENDED exim_employee;
SELECT * FROM exim_employee;