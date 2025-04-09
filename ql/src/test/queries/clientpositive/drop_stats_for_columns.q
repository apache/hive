create table t1(i int, j int, k int);

alter table t1 update statistics set('numRows'='10000', 'rawDataSize'='18000');
alter table t1 update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
alter table t1 update statistics for column j set('numDVs'='3000','numNulls'='40','highValue'='1200','lowValue'='2');
alter table t1 update statistics for column k set('numDVs'='4000','numNulls'='60','highValue'='1500','lowValue'='5');

describe formatted t1;
describe formatted t1 i;
describe formatted t1 j;
describe formatted t1 k;

-- drop stats from one column
alter table t1 drop statistics for columns i;
describe formatted t1 i;
describe formatted t1 j;
describe formatted t1 k;

alter table t1 update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');

-- drop stats from a list of columns
alter table t1 drop statistics for columns i, k;
describe formatted t1 i;
describe formatted t1 j;
describe formatted t1 k;

alter table t1 update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
alter table t1 update statistics for column k set('numDVs'='4000','numNulls'='60','highValue'='1500','lowValue'='5');

-- drop stats from all columns
alter table t1 drop statistics for columns;
describe formatted t1 i;
describe formatted t1 j;
describe formatted t1 k;

describe formatted t1;
