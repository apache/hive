create table t1_uq12(i int) partitioned by(j int);
alter table t1_uq12 add partition(j=1);
analyze table t1_uq12 compute statistics for columns;
alter table t1_uq12 partition(j=1) update statistics set('numRows'='10000', 'rawDataSize'='18000');
alter table t1_uq12 partition(j=1) update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
describe formatted t1_uq12 partition(j=1);
alter table t1_uq12 partition(j=1) drop statistics for column i;
describe formatted t1_uq12 partition(j=1) i;
