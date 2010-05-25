How to Hive
-----------

DISCLAIMER: This is a prototype version of Hive and is NOT production
quality. This is provided mainly as a way of illustrating the
capabilities of Hive and is provided as-is. However - we are working
hard to make Hive a production quality system. Hive has only been
tested on Unix(Linux) and Mac OS X using Java 1.6 for now - although
it may very well work on other similar platforms. It does not work on
Cygwin. Most of our testing has been on Hadoop 0.17 - so we would
advise running it against this version of hadoop - even though it may
compile/work against other versions

Useful mailing lists
--------------------

1. hive-user@hadoop.apache.org - To discuss and ask usage
   questions. Send an empty email to
   hive-user-subscribe@hadoop.apache.org in order to subscribe to this
   mailing list.

2. hive-core@hadoop.apache.org - For discussions around code, design
   and features. Send an empty email to
   hive-core-subscribe@hadoop.apache.org in order to subscribe to this
   mailing list.

3. hive-commits@hadoop.apache.org - In order to monitor the commits to
   the source repository. Send an empty email to
   hive-commits-subscribe@hadoop.apache.org in order to subscribe to
   this mailing list.

Downloading and building
------------------------

- svn co http://svn.apache.org/repos/asf/hadoop/hive/trunk hive_trunk
- cd hive_trunk
- hive_trunk> ant package -Dtarget.dir=<install-dir> -Dhadoop.version=0.17.2.1

If you do not specify a value for target.dir it will use the default
value build/dist. Then you can run the unit tests if you want to make
sure it is correctly built:

- hive_trunk> ant test -Dtarget.dir=<install-dir> -Dhadoop.version=0.17.2.1 \
              -logfile test.log

You can replace 0.17.2.1 with 0.18.3, 0.19.0, etc. to match the
version of hadoop that you are using. If you do not specify
hadoop.version it will use the default value specified in
build.properties file.


In the rest of the README, we use dist and <install-dir> interchangeably.

Running Hive
------------

Hive uses hadoop that means:
- you must have hadoop in your path OR
- export HADOOP=<hadoop-install-dir>/bin/hadoop

To use hive command line interface (cli) from the shell:
$ cd <install-dir>
$ bin/hive

Using Hive
----------

Configuration management overview
---------------------------------

- hive configuration is stored in <install-dir>/conf/hive-default.xml
  and log4j in hive-log4j.properties
- hive configuration is an overlay on top of hadoop - meaning the
  hadoop configuration variables are inherited by default.
- hive configuration can be manipulated by:
  o editing hive-default.xml and defining any desired variables
    (including hadoop variables) in it
  o from the cli using the set command (see below)
  o by invoking hive using the syntax:
     * bin/hive -hiveconf x1=y1 -hiveconf x2=y2
    this sets the variables x1 and x2 to y1 and y2


Error Logs
----------
Hive uses log4j for logging. By default logs are not emitted to the
console by the cli. They are stored in the file:
- /tmp/{user.name}/hive.log

If the user wishes - the logs can be emitted to the console by adding
the arguments shown below:
- bin/hive -hiveconf hive.root.logger=INFO,console

Note that setting hive.root.logger via the 'set' command does not
change logging properties since they are determined at initialization time.

Error logs are very useful to debug problems. Please send them with
any bugs (of which there are many!) to the JIRA at
https://issues.apache.org/jira/browse/HIVE.


DDL Operations
--------------

Creating Hive tables and browsing through them

hive> CREATE TABLE pokes (foo INT, bar STRING);

Creates a table called pokes with two columns, first being an
integer and other a string columns

hive> CREATE TABLE invites (foo INT, bar STRING) PARTITIONED BY (ds STRING);

Creates a table called invites with two columns and a partition column
called ds. The partition column is a virtual column.  It is not part
of the data itself, but is derived from the partition that a
particular dataset is loaded into.


By default tables are assumed to be of text input format and the
delimiters are assumed to be ^A(ctrl-a). We will be soon publish
additional commands/recipes to add binary (sequencefiles) data and
configurable delimiters etc.

hive> SHOW TABLES;

lists all the tables

hive> SHOW TABLES '.*s';

lists all the table that end with 's'. The pattern matching follows Java
regular expressions. Check out this link for documentation
http://java.sun.com/javase/6/docs/api/java/util/regex/Pattern.html

hive> DESCRIBE invites;

shows the list of columns

hive> DESCRIBE EXTENDED invites;

shows the list of columns plus any other meta information about the table

Altering tables. Table name can be changed and additional columns can be dropped

hive> ALTER TABLE pokes ADD COLUMNS (new_col INT);
hive> ALTER TABLE invites ADD COLUMNS (new_col2 INT COMMENT 'a comment');
hive> ALTER TABLE pokes REPLACE COLUMNS (c1 INT, c2 STRING);
hive> ALTER TABLE events RENAME TO 3koobecaf;

Dropping tables
hive> DROP TABLE pokes;


Metadata Store
--------------

Metadata is in an embedded Derby database whose location is determined by the
hive configuration variable named javax.jdo.option.ConnectionURL. By default
(see conf/hive-default.xml) - this location is ./metastore_db

Right now, in the default configuration, this metadata can only be seen by
one user at a time.

Metastore can be stored in any database that is supported by JPOX. The
location and the type of the RDBMS can be controlled by the two variables
'javax.jdo.option.ConnectionURL' and 'javax.jdo.option.ConnectionDriverName'.
Refer to JDO (or JPOX) documentation for more details on supported databases.
The database schema is defined in JDO metadata annotations file package.jdo
at src/contrib/hive/metastore/src/model.

In the future, the metastore itself can be a standalone server.


DML Operations
--------------

Loading data from flat files into Hive

hive> LOAD DATA LOCAL INPATH './examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;

Loads a file that contains two columns separated by ctrl-a into pokes
table.  'local' signifies that the input file is on the local
system. If 'local' is omitted then it looks for the file in HDFS.

The keyword 'overwrite' signifies that existing data in the table is
deleted.  If the 'overwrite' keyword is omitted - then data files are
appended to existing data sets.

NOTES:

- NO verification of data against the schema

- If the file is in hdfs it is moved into hive controlled file system
  namespace.  The root of the hive directory is specified by the
  option hive.metastore.warehouse.dir in hive-default.xml. We would
  advise that this directory be pre-existing before trying to create
  tables via Hive.

hive> LOAD DATA LOCAL INPATH './examples/files/kv2.txt' \
      OVERWRITE INTO TABLE invites PARTITION (ds='2008-08-15');

hive> LOAD DATA LOCAL INPATH './examples/files/kv3.txt' \
      OVERWRITE INTO TABLE invites PARTITION (ds='2008-08-08');

The two LOAD statements above load data into two different partitions
of the table invites. Table invites must be created as partitioned by
the key ds for this to succeed.

Loading/Extracting data using Queries
-------------------------------------

Runtime configuration
---------------------

- Hives queries are executed using map-reduce queries and as such the
  behavior of such queries can be controlled by the hadoop
  configuration variables

- The cli can be used to set any hadoop (or hive) configuration
  variable. For example:

   hive> SET mapred.job.tracker=myhost.mycompany.com:50030
   hive> SET - v

  The latter shows all the current settings. Without the v option only
  the variables that differ from the base hadoop configuration are
  displayed

- In particular the number of reducers should be set to a reasonable
  number to get good performance (the default is 1!)


EXAMPLE QUERIES
---------------

Some example queries are shown below. More are available in the hive
code: ql/src/test/queries/{positive,clientpositive}.

SELECTS and FILTERS
-------------------

hive> SELECT a.foo FROM invites a;

Select column 'foo' from all rows of invites table. The results are
not stored anywhere, but are displayed on the console.

Note that in all the examples that follow, INSERT (into a hive table,
local directory or HDFS directory) is optional.

hive> INSERT OVERWRITE DIRECTORY '/tmp/hdfs_out' SELECT a.* FROM invites a;

Select all rows from invites table into an HDFS directory. The result
data is in files (depending on the number of mappers) in that
directory.

NOTE: partition columns if any are selected by the use of *. They can
also be specified in the projection clauses.

hive> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/local_out' SELECT a.* FROM pokes a;

Select all rows from pokes table into a local directory

hive> INSERT OVERWRITE TABLE events SELECT a.* FROM profiles a;
hive> INSERT OVERWRITE TABLE events SELECT a.* FROM profiles a WHERE a.key < 100;
hive> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/reg_3' SELECT a.* FROM events a;
hive> INSERT OVERWRITE DIRECTORY '/tmp/reg_4' select a.invites, a.pokes FROM profiles a;
hive> INSERT OVERWRITE DIRECTORY '/tmp/reg_5' SELECT COUNT(1) FROM invites a;
hive> INSERT OVERWRITE DIRECTORY '/tmp/reg_5' SELECT a.foo, a.bar FROM invites a;
hive> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/sum' SELECT SUM(a.pc) FROM pc1 a;

Sum of a column. avg, min, max can also be used

NOTE: there are some flaws with the type system that cause doubles to
be returned with integer types would be expected. We expect to fix
these in the coming week.

GROUP BY
--------

hive> FROM invites a INSERT OVERWRITE TABLE events SELECT a.bar, count(1) WHERE a.foo > 0 GROUP BY a.bar;
hive> INSERT OVERWRITE TABLE events SELECT a.bar, count(1) FROM invites a WHERE a.foo > 0 GROUP BY a.bar;

NOTE: Currently Hive always uses two stage map-reduce for groupby
operation. This is to handle skews in input data. We will be
optimizing this in the coming weeks.

JOIN
----

hive> FROM pokes t1 JOIN invites t2 ON (t1.bar = t2.bar) INSERT OVERWRITE TABLE events SELECT t1.bar, t1.foo, t2.foo

MULTITABLE INSERT
-----------------
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3 PARTITION(ds='2008-04-08', hr='12') SELECT src.key WHERE src.key >= 200 and src.key < 300
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/dest4.out' SELECT src.value WHERE src.key >= 300

STREAMING
---------
hive> FROM invites a INSERT OVERWRITE TABLE events
    > MAP a.foo, a.bar USING '/bin/cat'
    > AS oof, rab WHERE a.ds > '2008-08-09';

This streams the data in the map phase through the script /bin/cat
(like hadoop streaming).  Similarly, streaming can be used on the
reduce side. Please look for files mapreduce*.q.

KNOWN BUGS/ISSUES
-----------------

* Hive cli may hang for a couple of minutes because of a bug in
  getting metadata from the derby database. let it run and you'll be
  fine!

* Hive cli creates derby.log in the directory from which it has been
  invoked.

* COUNT(*) does not work for now. Use COUNT(1) instead.

* ORDER BY not supported yet.

* CASE not supported yet.

* Only string and Thrift types (http://developers.facebook.com/thrift)
  have been tested.

* When doing Join, please put the table with big number of rows
  containing the same join key to the rightmost in the JOIN
  clause. Otherwise we may see OutOfMemory errors.

FUTURE FEATURES
---------------
* EXPLODE function to generate multiple rows from a column of list type.
* Table statistics for query optimization.

Developing Hive using Eclipse
------------------------
1. Follow the 3 steps in "Downloading and building" section above

2. Generate test files for eclipse:

   hive_trunk> cd metastore; ant model-jar; cd ..
   hive_trunk> cd ql; ant gen-test; cd ..
   hive_trunk> ant eclipse-files

   This will generate .project, .settings, .externalToolBuilders,
   .classpath, and a set of *.launch files for Eclipse-based
   debugging.

3. Launch Eclipse and import the Hive project by navigating to
   File->Import->General->Existing Projects menu and selecting
   the hive_trunk directory.

4. Run the CLI or a set of test cases by right clicking on one of the
   *.launch configurations and selection Run or Debug.

Development Tips
------------------------
* You may use the following line to test a specific testcase with a specific query file.
ant -Dhadoop.version='0.17.0' -Dtestcase=TestParse -Dqfile=udf4.q test
ant -Dhadoop.version='0.17.0' -Dtestcase=TestParseNegative -Dqfile=invalid_dot.q test
ant -Dhadoop.version='0.17.0' -Dtestcase=TestCliDriver -Dqfile=udf1.q test
ant -Dhadoop.version='0.17.0' -Dtestcase=TestNegativeCliDriver -Dqfile=invalid_tbl_name.q test
