Apache Hive (TM) 1.2.2
======================

The Apache Hive (TM) data warehouse software facilitates querying and
managing large datasets residing in distributed storage. Built on top
of Apache Hadoop (TM), it provides:

* Tools to enable easy data extract/transform/load (ETL)

* A mechanism to impose structure on a variety of data formats

* Access to files stored either directly in Apache HDFS (TM) or in other
  data storage systems such as Apache HBase (TM)

* Query execution using Apache Hadoop MapReduce, Apache Tez
  or Apache Spark frameworks.

Hive implements a dialect of SQL (Hive QL) that focuses on analytics
and presents a rich set of SQL semantics including OLAP functions,
subqueries, common table expressions and more. Hive allows SQL
developers or users with SQL tools to easily query, analyze and
process data stored in Hadoop.
Hive also allows programmers familiar with the MapReduce framework
to plug in their custom mappers and reducers to perform more
sophisticated analysis that may not be supported by the built-in
capabilities of the language. QL can also be extended with custom
scalar functions (UDF's), aggregations (UDAF's), and table
functions (UDTF's).

Hive users have a choice of 3 runtimes when executing SQL queries.
Users can choose between Apache Hadoop MapReduce, Apache Tez or
Apache Spark frameworks as their execution backend. MapReduce is a
mature framework that is proven at large scales. However, MapReduce
is a purely batch framework, and queries using it may experience
higher latencies (tens of seconds), even over small datasets. Apache
Tez is designed for interactive query, and has substantially reduced
overheads versus MapReduce. Apache Spark is a cluster computing
framework that's built outside of MapReduce, but on top of HDFS,
with a notion of composable and transformable distributed collection
of items called Resilient Distributed Dataset (RDD) which allows
processing and analysis without traditional intermediate stages that
MapReduce introduces.

Users are free to switch back and forth between these frameworks
at any time. In each case, Hive is best suited for use cases
where the amount of data processed is large enough to require a
distributed system.

Hive is not designed for online transaction processing and does
not support row level insert/updates. It is best used for batch
jobs over large sets of immutable data (like web logs). What
Hive values most are scalability (scale out with more machines
added dynamically to the Hadoop cluster), extensibility (with
MapReduce framework and UDF/UDAF/UDTF), fault-tolerance, and
loose-coupling with its input formats.


General Info
============

For the latest information about Hive, please visit out website at:

  http://hive.apache.org/


Getting Started
===============

- Installation Instructions and a quick tutorial:
  https://cwiki.apache.org/confluence/display/Hive/GettingStarted

- A longer tutorial that covers more features of HiveQL:
  https://cwiki.apache.org/confluence/display/Hive/Tutorial

- The HiveQL Language Manual:
  https://cwiki.apache.org/confluence/display/Hive/LanguageManual


Requirements
============

- Java 1.7

- Hadoop 1.x, 2.x


Upgrading from older versions of Hive
=====================================

- Hive 1.2.2 does not include changes to the MetaStore schema. If you are
  upgrading from an earlier version of Hive prior to Hive 1.2.0, it
  is imperative that you upgrade the MetaStore schema by running
  the appropriate schema upgrade scripts located in the
  scripts/metastore/upgrade directory.

- We have provided upgrade scripts for MySQL, PostgreSQL, Oracle,
  Microsoft SQL Server, and Derby databases. If you are using a
  different database for your MetaStore you will need to provide
  your own upgrade script.

Useful mailing lists
====================

1. user@hive.apache.org - To discuss and ask usage questions. Send an
   empty email to user-subscribe@hive.apache.org in order to subscribe
   to this mailing list.

2. dev@hive.apache.org - For discussions about code, design and features.
   Send an empty email to dev-subscribe@hive.apache.org in order to
   subscribe to this mailing list.

3. commits@hive.apache.org - In order to monitor commits to the source
   repository. Send an empty email to commits-subscribe@hive.apache.org
   in order to subscribe to this mailing list.
