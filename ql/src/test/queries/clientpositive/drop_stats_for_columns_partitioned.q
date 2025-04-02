create table t1(i int, j int, k int) partitioned by (l int, m int);

alter table t1 add partition(l=1, m=10);
alter table t1 add partition(l=1, m=20);
alter table t1 add partition(l=2, m=10);
alter table t1 add partition(l=2, m=20);

----------------- set stats -----------------
alter table t1 partition(l=1, m=10) update statistics set('numRows'='10000', 'rawDataSize'='18000');
alter table t1 partition(l=1, m=20) update statistics set('numRows'='11000', 'rawDataSize'='19000');
alter table t1 partition(l=2, m=10) update statistics set('numRows'='12000', 'rawDataSize'='20000');
alter table t1 partition(l=2, m=20) update statistics set('numRows'='13000', 'rawDataSize'='21000');

alter table t1 partition(l=1, m=10) update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
alter table t1 partition(l=1, m=10) update statistics for column j set('numDVs'='2501','numNulls'='51','highValue'='1001','lowValue'='1');
alter table t1 partition(l=1, m=10) update statistics for column k set('numDVs'='2502','numNulls'='52','highValue'='1002','lowValue'='2');

alter table t1 partition(l=1, m=20) update statistics for column i set('numDVs'='2510','numNulls'='60','highValue'='1010','lowValue'='0');
alter table t1 partition(l=1, m=20) update statistics for column j set('numDVs'='2511','numNulls'='61','highValue'='1011','lowValue'='1');
alter table t1 partition(l=1, m=20) update statistics for column k set('numDVs'='2512','numNulls'='62','highValue'='1012','lowValue'='2');

alter table t1 partition(l=2, m=10) update statistics for column i set('numDVs'='3500','numNulls'='150','highValue'='2000','lowValue'='0');
alter table t1 partition(l=2, m=10) update statistics for column j set('numDVs'='3501','numNulls'='151','highValue'='2001','lowValue'='1');
alter table t1 partition(l=2, m=10) update statistics for column k set('numDVs'='3502','numNulls'='152','highValue'='2002','lowValue'='2');

alter table t1 partition(l=2, m=20) update statistics for column i set('numDVs'='3510','numNulls'='160','highValue'='2010','lowValue'='0');
alter table t1 partition(l=2, m=20) update statistics for column j set('numDVs'='3511','numNulls'='161','highValue'='2011','lowValue'='1');
alter table t1 partition(l=2, m=20) update statistics for column k set('numDVs'='3512','numNulls'='162','highValue'='2012','lowValue'='2');
-----------------------------------------------

---- initial DESCs ----
describe formatted t1;
describe formatted t1 partition(l=1, m=10) i;
describe formatted t1 partition(l=1, m=10) j;
describe formatted t1 partition(l=1, m=10) k;

describe formatted t1 partition(l=1, m=20) i;
describe formatted t1 partition(l=1, m=20) j;
describe formatted t1 partition(l=1, m=20) k;

describe formatted t1 partition(l=2, m=10) i;
describe formatted t1 partition(l=2, m=10) j;
describe formatted t1 partition(l=2, m=10) k;

describe formatted t1 partition(l=2, m=20) i;
describe formatted t1 partition(l=2, m=20) j;
describe formatted t1 partition(l=2, m=20) k;
------------------------

---- drop stats for one column from one partition ----
alter table t1 partition(l=1, m=10) drop statistics for columns i;
describe formatted t1 partition(l=1, m=10) i;
describe formatted t1 partition(l=1, m=10) j;
describe formatted t1 partition(l=1, m=10) k;

alter table t1 partition(l=1, m=10) update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
------------------------------------------------------

---- drop stats for a list of columns from one partition ----
alter table t1 partition(l=2, m=10) drop statistics for columns i, k;
describe formatted t1 partition(l=2, m=10) i;
describe formatted t1 partition(l=2, m=10) j;
describe formatted t1 partition(l=2, m=10) k;

alter table t1 partition(l=2, m=10) update statistics for column i set('numDVs'='3500','numNulls'='150','highValue'='2000','lowValue'='0');
alter table t1 partition(l=2, m=10) update statistics for column k set('numDVs'='3502','numNulls'='152','highValue'='2002','lowValue'='2');
-------------------------------------------------------------

---- drop stats for all columns from one partition ----
alter table t1 partition(l=2, m=20) drop statistics for columns;
describe formatted t1 partition(l=2, m=20) i;
describe formatted t1 partition(l=2, m=20) j;
describe formatted t1 partition(l=2, m=20) k;

alter table t1 partition(l=2, m=20) update statistics for column i set('numDVs'='3510','numNulls'='160','highValue'='2010','lowValue'='0');
alter table t1 partition(l=2, m=20) update statistics for column j set('numDVs'='3511','numNulls'='161','highValue'='2011','lowValue'='1');
alter table t1 partition(l=2, m=20) update statistics for column k set('numDVs'='3512','numNulls'='162','highValue'='2012','lowValue'='2');
-------------------------------------------------------

---- drop stats for one column from all partitions ----
alter table t1 drop statistics for columns i;
describe formatted t1 partition(l=1, m=10) i;
describe formatted t1 partition(l=1, m=20) i;
describe formatted t1 partition(l=2, m=10) i;
describe formatted t1 partition(l=2, m=20) i;

alter table t1 partition(l=1, m=10) update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
alter table t1 partition(l=1, m=20) update statistics for column i set('numDVs'='2510','numNulls'='60','highValue'='1010','lowValue'='0');
alter table t1 partition(l=2, m=10) update statistics for column i set('numDVs'='3500','numNulls'='150','highValue'='2000','lowValue'='0');
alter table t1 partition(l=2, m=20) update statistics for column i set('numDVs'='3510','numNulls'='160','highValue'='2010','lowValue'='0');
-------------------------------------------------------

---- drop stats for a list of columns from all partitions ----
alter table t1 drop statistics for columns i, k;
describe formatted t1 partition(l=1, m=10) i;
describe formatted t1 partition(l=1, m=20) i;
describe formatted t1 partition(l=2, m=10) i;
describe formatted t1 partition(l=2, m=20) i;
describe formatted t1 partition(l=1, m=10) k;
describe formatted t1 partition(l=1, m=20) k;
describe formatted t1 partition(l=2, m=10) k;
describe formatted t1 partition(l=2, m=20) k;

alter table t1 partition(l=1, m=10) update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
alter table t1 partition(l=1, m=20) update statistics for column i set('numDVs'='2510','numNulls'='60','highValue'='1010','lowValue'='0');
alter table t1 partition(l=2, m=10) update statistics for column i set('numDVs'='3500','numNulls'='150','highValue'='2000','lowValue'='0');
alter table t1 partition(l=2, m=20) update statistics for column i set('numDVs'='3510','numNulls'='160','highValue'='2010','lowValue'='0');
alter table t1 partition(l=1, m=10) update statistics for column k set('numDVs'='2502','numNulls'='52','highValue'='1002','lowValue'='2');
alter table t1 partition(l=1, m=20) update statistics for column k set('numDVs'='2512','numNulls'='62','highValue'='1012','lowValue'='2');
alter table t1 partition(l=2, m=10) update statistics for column k set('numDVs'='3502','numNulls'='152','highValue'='2002','lowValue'='2');
alter table t1 partition(l=2, m=20) update statistics for column k set('numDVs'='3512','numNulls'='162','highValue'='2012','lowValue'='2');
--------------------------------------------------------------

---- drop stats for all columns from all partitions ----
alter table t1 drop statistics for columns;

describe formatted t1 partition(l=1, m=10) i;
describe formatted t1 partition(l=1, m=10) j;
describe formatted t1 partition(l=1, m=10) k;

describe formatted t1 partition(l=1, m=20) i;
describe formatted t1 partition(l=1, m=20) j;
describe formatted t1 partition(l=1, m=20) k;

describe formatted t1 partition(l=2, m=10) i;
describe formatted t1 partition(l=2, m=10) j;
describe formatted t1 partition(l=2, m=10) k;

describe formatted t1 partition(l=2, m=20) i;
describe formatted t1 partition(l=2, m=20) j;
describe formatted t1 partition(l=2, m=20) k;
--------------------------------------------------------
