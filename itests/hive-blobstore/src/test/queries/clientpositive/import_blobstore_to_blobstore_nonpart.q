-- Check we can create a non partitioned table in the warehouse, 
-- export it to a blobstore, and then import the
-- table using the blobstore as target location
DROP TABLE exim_employee;
CREATE TABLE exim_employee (emp_id int COMMENT "employee id")
COMMENT "employee table"
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/test.dat"
INTO TABLE exim_employee;

DESCRIBE EXTENDED exim_employee;
SELECT * FROM exim_employee;

dfs -rm -r -f ${hiveconf:test.blobstore.path.unique}/import_blobstore_to_blobstore_nonpart/export/exim_employee;
EXPORT TABLE exim_employee
TO '${hiveconf:test.blobstore.path.unique}/import_blobstore_to_blobstore_nonpart/export/exim_employee';

DROP TABLE exim_employee;
dfs -rm -r -f ${hiveconf:test.blobstore.path.unique}/import_blobstore_to_blobstore_nonpart/import/exim_employee;
IMPORT FROM '${hiveconf:test.blobstore.path.unique}/import_blobstore_to_blobstore_nonpart/export/exim_employee'
LOCATION '${hiveconf:test.blobstore.path.unique}/import_blobstore_to_blobstore_nonpart/import/exim_employee';

DESCRIBE EXTENDED exim_employee;
SELECT * FROM exim_employee;