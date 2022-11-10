
  create external table test(
      strcol string,
      intcol integer
  ) partitioned by (pcol int)
  stored as parquet
  location '../../data/files/parquet_partition';

 msck repair table test;

 select * from test;

 explain select * from test where pcol=100 and intcol=2;
 select * from test where pcol=100 and intcol=2;

 explain select * from test where PCOL=200 and intcol=3;
 select * from test where PCOL=200 and intcol=3;

 explain select * from test where `pCol`=300 and intcol=5;
 select * from test where `pCol`=300 and intcol=5;