Apache Hive (TM)
================
[![Master Build Status](https://travis-ci.org/apache/hive.svg?branch=master)](https://travis-ci.org/apache/hive/branches)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.hive/hive/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.hive%22)

The Apache Hive (TM) data warehouse software facilitates reading,
writing, and managing large datasets residing in distributed storage
using SQL. Built on top of Apache Hadoop (TM), it provides:

* Tools to enable easy access to data via SQL, thus enabling data
  warehousing tasks such as extract/transform/load (ETL), reporting,
  and data analysis

* A mechanism to impose structure on a variety of data formats

* Access to files stored either directly in Apache HDFS (TM) or in other
  data storage systems such as Apache HBase (TM)

* Query execution using Apache Hadoop MapReduce or Apache Tez frameworks.

Hive provides standard SQL functionality, including many of the later
2003 and 2011 features for analytics.  These include OLAP functions,
subqueries, common table expressions, and more.  Hive's SQL can also be
extended with user code via user defined functions (UDFs), user defined
aggregates (UDAFs), and user defined table functions (UDTFs).

Hive users can choose between Apache Hadoop MapReduce or Apache Tez
frameworks as their execution backend. Note that MapReduce framework
has been deprecated since Hive 2, and Apache Tez is recommended. MapReduce
is a mature framework that is proven at large scales. However, MapReduce
is a purely batch framework, and queries using it may experience
higher latencies (tens of seconds), even over small datasets. Apache
Tez is designed for interactive query, and has substantially reduced
overheads versus MapReduce.

Users are free to switch back and forth between these frameworks
at any time. In each case, Hive is best suited for use cases
where the amount of data processed is large enough to require a
distributed system.

Hive is not designed for online transaction processing. It is best used
for traditional data warehousing tasks.  Hive is designed to maximize
scalability (scale out with more machines added dynamically to the Hadoop
cluster), performance, extensibility, fault-tolerance, and
loose-coupling with its input formats.


General Info
============

For the latest information about Hive, please visit out website at:

  http://hive.apache.org/


Getting Started
===============

- Installation Instructions and a quick tutorial:
  https://cwiki.apache.org/confluence/display/Hive/GettingStarted

- Instructions to build Hive from source:
  https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-BuildingHivefromSource

- A longer tutorial that covers more features of HiveQL:
  https://cwiki.apache.org/confluence/display/Hive/Tutorial

- The HiveQL Language Manual:
  https://cwiki.apache.org/confluence/display/Hive/LanguageManual


Requirements
============

Java
------

| Hive Version  | Java Version  |
| ------------- |:-------------:|
| Hive 1.0      | Java 6        |
| Hive 1.1      | Java 6        |
| Hive 1.2      | Java 7        |
| Hive 2.x      | Java 7        |
| Hive 3.x      | Java 8        |
| Hive 4.x      | Java 8        |


Hadoop
------

- Hadoop 1.x, 2.x
- Hadoop 3.x (Hive 3.x)
- Hadoop 3.3.6 (Hive 4.x)


Upgrading from older versions of Hive
=====================================

- Hive includes changes to the MetaStore schema. If
  you are upgrading from an earlier version of Hive it is imperative
  that you upgrade the MetaStore schema by running the appropriate
  schema upgrade scripts located in the scripts/metastore/upgrade
  directory.

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
