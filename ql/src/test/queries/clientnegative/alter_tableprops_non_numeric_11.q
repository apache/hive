create table stat_test (a int);

alter table stat_test set TBLPROPERTIES('numFiles'='1', 'numRows'='1', 'totalSize'='1', 'rawDataSize'='1', 'numFilesErasureCoded'='NaN');
