create table t1_uq12(i int, j int);
alter table t1_uq12 update statistics set('numRows'='10000', 'rawDataSize'='18000');
alter table t1_uq12 update statistics for column i set('numDVs'='2500','numNulls'='50','highValue'='1000','lowValue'='0');
describe formatted t1_uq12 i;
alter table t1_uq12 drop statistics for column i;
describe formatted t1_uq12 i;
