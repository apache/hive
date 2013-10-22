Apache Hive (TM) @VERSION@
======================

The Apache Hive (TM) data warehouse software facilitates querying and
managing large datasets residing in distributed storage. Built on top
of Apache Hadoop (TM), it provides:

* Tools to enable easy data extract/transform/load (ETL)

* A mechanism to impose structure on a variety of data formats

* Access to files stored either directly in Apache HDFS (TM) or in other
  data storage systems such as Apache HBase (TM)

* Query execution via MapReduce

Hive defines a simple SQL-like query language, called QL, that enables
users familiar with SQL to query the data. At the same time, this
language also allows programmers who are familiar with the MapReduce
framework to be able to plug in their custom mappers and reducers to
perform more sophisticated analysis that may not be supported by the
built-in capabilities of the language. QL can also be extended with
custom scalar functions (UDF's), aggregations (UDAF's), and table
functions (UDTF's).

Please note that Hadoop is a batch processing system and Hadoop jobs
tend to have high latency and incur substantial overheads in job
submission and scheduling. Consequently the average latency for Hive
queries is generally very high (minutes) even when data sets involved
are very small (say a few hundred megabytes). As a result it cannot be
compared with systems such as Oracle where analyses are conducted on a
significantly smaller amount of data but the analyses proceed much
more iteratively with the response times between iterations being less
than a few minutes. Hive aims to provide acceptable (but not optimal)
latency for interactive data browsing, queries over small data sets or
test queries.

Hive is not designed for online transaction processing and does not
support real-time queries or row level insert/updates. It is best used
for batch jobs over large sets of immutable data (like web logs). What
Hive values most are scalability (scale out with more machines added
dynamically to the Hadoop cluster), extensibility (with MapReduce
framework and UDF/UDAF/UDTF), fault-tolerance, and loose-coupling with
its input formats.


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

- Java 1.6

- Hadoop 0.20.x (x >= 1)


Upgrading from older versions of Hive
=====================================

- Hive @VERSION@ includes changes to the MetaStore schema. If
  you are upgrading from an earlier version of Hive it is imperative
  that you upgrade the MetaStore schema by running the appropriate
  schema upgrade scripts located in the scripts/metastore/upgrade
  directory.

- We have provided upgrade scripts for MySQL, PostgreSQL, Oracle and Derby
  databases. If you are using a different database for your MetaStore
  you will need to provide your own upgrade script.

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
