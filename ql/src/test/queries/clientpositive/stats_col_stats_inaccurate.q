set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
set hive.stats.fetch.column.stats=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- Check partitioned tables on float/double columns with Infinity/NaN on inaccurate stats.

create table stats_t1(
  c_double double,
  c_float float,
  c_str string)
partitioned by (p int)
STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table stats_t1 partition(p=1) values
  (cast('Infinity' as double), cast('Infinity' as float), 'row1'),
  (cast('-Infinity' as double), cast('-Infinity' as float), 'row2'),
  (cast('NAN' as double), cast('NaN' as float), 'row3'),
  (cast(1234 as double), 123.456, 'row4');

describe formatted stats_t1 partition(p=1) c_double;
describe formatted stats_t1 partition(p=1) c_float;
describe formatted stats_t1 partition(p=1);


-- Check non-partitioned tables on flout/double columns with Infinity/NaN on inaccurate stats.

create table stats_t2(
  c_double double,
  c_float float,
  c_str string)
stored as ORC TBLPROPERTIES ('transactional'='true');

insert into table stats_t2 values
  (cast('Infinity' as double), cast('Infinity' as float), 'row1'),
  (cast('-Infinity' as double), cast('-Infinity' as float), 'row2'),
  (cast('NAN' as double), cast('NaN' as float), 'row3'),
  (cast(1234 as double), 123.456, 'row4');
analyze table stats_t2 compute statistics for columns;


describe formatted stats_t2  c_double;
describe formatted stats_t2 c_float;
describe formatted stats_t2;

-- All columns fail with UnsupportedDoubleException
create table stats_t3(
  a double,
  b float)
partitioned by (p int)
stored as ORC;

insert into table stats_t3 partition(p=1) values
  (cast('Infinity' as double), cast('Infinity' as float)),
  (cast('-Infinity' as double), cast('-Infinity' as float)),
  (cast('NaN' as double), cast('NaN' as float));
describe formatted stats_t3 partition(p=1) a;
describe formatted stats_t3 partition(p=1) b;
describe formatted stats_t3 partition(p=1);

-- Multiple partitions with different columns failed with UnsupportedDoubleException

create table stats_t4(
  a double,
  b float)
partitioned by (p int)
stored as ORC;

-- a fails with UnsupportedDoubleException, b normal
insert into table stats_t4 partition(p=1) values
  (cast('Infinity' as double), 3.14),
  (cast('-Infinity' as double), 2.72),
  (cast('NaN' as double), 1.618);

-- a normal, b fails with UnsupportedDoubleException
insert into table stats_t4 partition(p=2) values
  (42.0, cast('Infinity' as float)),
  (17.3, cast('-Infinity' as float)),
  (5.4, cast('NaN' as float));

-- both a and b fail with UnsupportedDoubleException
insert into table stats_t4 partition(p=3) values
  (cast('Infinity' as double), cast('-Infinity' as float)),
  (cast('-Infinity' as double), cast('Infinity' as float)),
  (cast('NaN' as double), cast('NaN' as float));

-- both a and b normal
insert into table stats_t4 partition(p=4) values
  (1.0, 2.0),
  (3.0, 4.0);

describe formatted stats_t4 partition(p=1);
describe formatted stats_t4 partition(p=2);
describe formatted stats_t4 partition(p=3);
describe formatted stats_t4 partition(p=4);
