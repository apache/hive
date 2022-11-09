
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
