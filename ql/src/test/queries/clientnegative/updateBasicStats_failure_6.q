create table stat_test (a int);

-- this should fail, only numRows and rawDataSize should be updateable through update statistics
alter table stat_test update statistics set ('numFiles'='1');