-- Check we can create a partitioned table in the warehouse, 
-- export it to the local filesystem, and then import its 
-- different partitions using a blobstore as target location
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

dfs -rm -r -f ${system:build.test.dir}/import_addpartition_local_to_blobstore/export/exim_employee;
EXPORT TABLE exim_employee
TO 'file://${system:build.test.dir}/import_addpartition_local_to_blobstore/export/exim_employee';

DROP TABLE exim_employee;
dfs -rm -r -f ${hiveconf:test.blobstore.path.unique}/import_addpartition_local_to_blobstore/import/exim_employee;
IMPORT TABLE exim_employee PARTITION (emp_country='us')
FROM 'file://${system:build.test.dir}/import_addpartition_local_to_blobstore/export/exim_employee'
LOCATION '${hiveconf:test.blobstore.path.unique}/import_addpartition_local_to_blobstore/import/exim_employee';

DESCRIBE EXTENDED exim_employee;
SELECT * FROM exim_employee;

IMPORT TABLE exim_employee PARTITION (emp_country='cz')
FROM 'file://${system:build.test.dir}/import_addpartition_local_to_blobstore/export/exim_employee'
LOCATION '${hiveconf:test.blobstore.path.unique}/import_addpartition_local_to_blobstore/import/exim_employee';

DESCRIBE EXTENDED exim_employee;
SELECT * FROM exim_employee;

IMPORT TABLE exim_employee PARTITION (emp_country='in')
FROM 'file://${system:build.test.dir}/import_addpartition_local_to_blobstore/export/exim_employee'
LOCATION '${hiveconf:test.blobstore.path.unique}/import_addpartition_local_to_blobstore/import/exim_employee';

DESCRIBE EXTENDED exim_employee;
SELECT * FROM exim_employee;