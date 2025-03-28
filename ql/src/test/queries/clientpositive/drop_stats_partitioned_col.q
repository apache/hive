create table t1_uq12(i int) partitioned by(j int);
alter table t1_uq12 add partition(j=1);
alter table t1_uq12 add partition(j=2);

alter table t1_uq12 partition(j=1) update statistics set('numRows'='10000', 'rawDataSize'='18000');
alter table t1_uq12 partition(j=1) update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
alter table t1_uq12 partition(j=2) update statistics set('numRows'='20000', 'rawDataSize'='40000');
alter table t1_uq12 partition(j=2) update statistics for column i set('numDVs'='3000','numNulls'='60','highValue'='2000','lowValue'='100');

describe formatted t1_uq12 partition(j=1);
describe formatted t1_uq12 partition(j=1) i;
describe formatted t1_uq12 partition(j=2);
describe formatted t1_uq12 partition(j=2) i;
describe formatted t1_uq12;

alter table t1_uq12 partition(j=1) drop statistics for column i;

describe formatted t1_uq12 partition(j=1);
describe formatted t1_uq12 partition(j=1) i;
describe formatted t1_uq12 partition(j=2);
describe formatted t1_uq12 partition(j=2) i;
describe formatted t1_uq12;
