Apache Hive 0.6.0
=================

Hive is a data warehouse system for Hadoop that facilitates
easy data summarization, ad-hoc querying and analysis of large
datasets stored in Hadoop compatible file systems. Hive provides a
mechanism to put structure on this data and query the data using a
SQL-like language called HiveQL. At the same time this language also
allows traditional map/reduce programmers to plug in their custom
mappers and reducers when it is inconvenient or inefficient to express
this logic in HiveQL.

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

and our wiki at:

  http://wiki.apache.org/hadoop/Hive/


Getting Started
===============

- Installation Instructions and a quick tutorial:
  http://wiki.apache.org/hadoop/Hive/GettingStarted

- A longer tutorial that covers more features of HiveQL:
  http://wiki.apache.org/hadoop/Hive/Tutorial

- The HiveQL Language Manual:
  http://wiki.apache.org/hadoop/Hive/LanguageManual


Requirements
============

- Java 1.6

- Hadoop 0.17, 0.18, 0.19, or 0.20.

  *NOTE*: We strongly recommend that you use Hadoop 0.20
  since the majority of our testing is done against this
  version and because support for pre-0.20 versions of
  Hadoop will be dropped in Hive 0.7.


Upgrading from older versions of Hive
=====================================

- Hive 0.6.0 includes changes to the MetaStore schema. If
  you are upgrading from an earlier version of Hive it is
  imperative that you upgrade the MetaStore schema by
  running the appropriate schema upgrade script located in
  the scripts/metastore/upgrade directory.

  We have provided upgrade scripts for Derby, MySQL, and PostgreSQL
  databases. If you are using a different database for your MetaStore
  you will need to provide your own upgrade script.

- Hive 0.6.0 includes new configuration properties. If you
  are upgrading from an earlier version of Hive it is imperative
  that you replace all of the old copies of the hive-default.xml
  configuration file with the new version located in the conf/
  directory.


Useful mailing lists
====================

1. user@hive.apache.org - To discuss and ask usage questions. Send an
   empty email to user-subscribe@hive.apache.org in order to subscribe
   to this mailing list.

2. dev@hive.apache.org - For discussions about code, design and features.
   Send an empty email to dev-subscribe@hive.apache.org in order to subscribe
   to this mailing list.

3. commits@hive.apache.org - In order to monitor commits to the source
   repository. Send an empty email to commits-subscribe@hive.apache.org
   in order to subscribe to this mailing list.
