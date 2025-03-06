create external table stat_test (a int);

alter table stat_test set TBLPROPERTIES('numFiles'='1', 'numRows'='2', 'totalSize'='3', 'rawDataSize'='4', 'numFilesErasureCoded'='5', 'STATS_GENERATED_VIA_STATS_TASK'='true');

describe formatted stat_test;