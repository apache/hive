<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Kafka Storage Handler Module

Storage Handler that allows users to connect/analyze/transform Kafka topics.
The workflow is as follows:
- First, the user will create an external table that is a view over one Kafka topic
- Second, the user will be able to run any SQL query including write back to the same table or different Kafka backed table

## Kafka Management

Kafka Java client version: 2.x

This handler does not commit offsets of topic partition reads either using the intrinsic Kafka capability or in an external
storage.  This means a query over a Kafka topic backed table will be a full topic read unless partitions are filtered
manually, via SQL, by the methods described below. In the ETL section, a method for storing topic offsets in Hive tables
is provided for tracking consumer position but this is not a part of the handler itself.

## Usage

### Create Table
Use the following statement to create a table:

```sql
CREATE EXTERNAL TABLE 
  kafka_table (
    `timestamp` TIMESTAMP,
    `page` STRING,
    `newPage` BOOLEAN,
    `added` INT, 
    `deleted` BIGINT,
    `delta` DOUBLE)
STORED BY 
  'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES ( 
  "kafka.topic" = "test-topic",
  "kafka.bootstrap.servers" = "localhost:9092");
```

The table property `kafka.topic` is the Kafka topic to connect to and `kafka.bootstrap.servers` is the Kafka broker connection string.
Both properties are mandatory.
On the write path if such a topic does not exist the topic will be created if Kafka broker admin policy allows for 
auto topic creation.

By default the serializer and deserializer is JSON, specifically `org.apache.hadoop.hive.serde2.JsonSerDe`.

If you want to change the serializer/deserializer classes you can update the TBLPROPERTIES with SQL syntax `ALTER TABLE`.

```sql
ALTER TABLE 
  kafka_table 
SET TBLPROPERTIES (
  "kafka.serde.class" = "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
```

If you use Confluent's Avro serialzier or deserializer with the Confluent Schema Registry, you will need to remove five bytes from the beginning of each message. These five bytes represent [a magic byte and a four-byte schema ID from the registry.](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
This can be done by setting `"avro.serde.type"="skip"` and `"avro.serde.skip.bytes"="5"`. In this case it is also recommended to set the Avro schema either via `"avro.schema.url"="http://hostname/SimpleDocument.avsc"` or `"avro.schema.literal"="{"type" : "record","name" : "SimpleRecord","..."}`. If both properties are set then `avro.schema.literal` has higher priority.
 
List of supported serializers and deserializers:

|Supported Serializers and Deserializers|
|-----|
|org.apache.hadoop.hive.serde2.JsonSerDe|
|org.apache.hadoop.hive.serde2.OpenCSVSerde|
|org.apache.hadoop.hive.serde2.avro.AvroSerDe|
|org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe|
|org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe|

#### Table Definitions 
In addition to the user defined column schema, this handler will append additional columns allowing
the user to query the Kafka metadata fields:
- `__key` Kafka record key (byte array)
- `__partition` Kafka record partition identifier (int 32)
- `__offset` Kafka record offset (int 64)
- `__timestamp` Kafka record timestamp (int 64)

 
### Query Table

List the table properties and all the partition/offsets information for the topic. 
```sql
DESCRIBE EXTENDED 
  kafka_table;
```

Count the number of records where the record timestamp is within the last 10 minutes of query execution time.

```sql
SELECT 
  COUNT(*)
FROM 
  kafka_table 
WHERE 
  `__timestamp` >  1000 * TO_UNIX_TIMESTAMP(CURRENT_TIMESTAMP - INTERVAL '10' MINUTES);
```

The storage handler allows these metadata fields to filter push-down read optimizations to Kafka.
For instance, the query above will only read the records with timestamp satisfying the filter predicate. 
Please note that such time based filtering (Kafka consumer partition seek) is only viable if the Kafka broker 
version allows time based look up (Kafka 0.11 or later versions)

In addition to **time based filtering**, the storage handler reader is able to filter based on a
particular partition offset using the SQL WHERE clause.
Currently supports operators `OR` and `AND` with comparison operators `<`, `<=`, `>=`, `>`. 

#### Metadata Query Examples

```sql
SELECT
  COUNT(*)
FROM 
  kafka_table
WHERE 
  (`__offset` < 10 AND `__offset` >3 AND `__partition` = 0)
  OR 
  (`__partition` = 0 AND `__offset` < 105 AND `__offset` > 99)
  OR (`__offset` = 109);
```

Keep in mind that partitions can grow and shrink within the Kafka cluster without the consumer's knowledge. This 
partition and offset capability is good for replay of specific partitions when the consumer knows that something has 
gone wrong down stream or replay is required.  Apache Hive users may or may not understand the underlying architecture 
of Kafka therefore, filtering on the record timestamp metadata column is arguably the best filter to use since it 
requires no partition knowledge. 

The user can define a view to take of the last 15 minutes and mask what ever column as follows:

```sql
CREATE VIEW 
  last_15_minutes_of_kafka_table 
AS 
SELECT 
  `timestamp`,
  `user`, 
  `delta`, 
  `added` 
FROM 
  kafka_table 
WHERE 
  `__timestamp` >  1000 * TO_UNIX_TIMESTAMP(CURRENT_TIMESTAMP - INTERVAL '15' MINUTES);
```

Join the Kafka topic to Hive table. For instance, assume you want to join the last 15 minutes of the topic to 
a dimension table with the following example: 

```sql
CREATE TABLE 
  user_table (
  `user` STRING, 
  `first_name` STRING, 
  `age` INT, 
  `gender` STRING, 
  `comments` STRING ) 
STORED AS ORC;
```

Join the view of the last 15 minutes to `user_table`, group by the `gender` column and compute aggregates
over metrics from the fact and dimension tables.

```sql
SELECT 
  SUM(`added`) AS `added`, 
  SUM(`deleted`) AS `deleted`, 
  AVG(`delta`) AS `delta`, 
  AVG(`age`) AS `avg_age`, 
  `gender` 
FROM 
  last_15_minutes_of_kafka_table 
JOIN 
  user_table ON 
    last_15_minutes_of_kafka_table.`user` = user_table.`user`
GROUP BY 
  `gender` 
LIMIT 10;
```

In cases where you want to perform some ad-hoc analysis over the last 15 minutes of topic data,
you can join it on itself. In the following example, we show how you can perform classical 
user retention analysis over the Kafka topic.

```sql
-- Topic join over the view itself
-- The example is adapted from https://www.periscopedata.com/blog/how-to-calculate-cohort-retention-in-sql
-- Assuming l15min_wiki is a view of the last 15 minutes based on the topic's timestamp record metadata

SELECT 
  COUNT(DISTINCT `activity`.`user`) AS `active_users`, 
  COUNT(DISTINCT `future_activity`.`user`) AS `retained_users`
FROM 
  l15min_wiki AS activity
LEFT JOIN 
  l15min_wiki AS future_activity
ON
  activity.`user` = future_activity.`user`
AND 
  activity.`timestamp` = future_activity.`timestamp` - INTERVAL '5' MINUTES;

--  Topic to topic join
-- Assuming wiki_kafka_hive is the entire topic

SELECT 
  FLOOR_HOUR(activity.`timestamp`), 
  COUNT(DISTINCT activity.`user`) AS `active_users`, 
  COUNT(DISTINCT future_activity.`user`) AS retained_users
FROM 
  wiki_kafka_hive AS activity
LEFT JOIN 
  wiki_kafka_hive AS future_activity 
ON
  activity.`user` = future_activity.`user`
AND 
  activity.`timestamp` = future_activity.`timestamp` - INTERVAL '1' HOUR 
GROUP BY 
  FLOOR_HOUR(activity.`timestamp`); 
```

# Configuration

## Table Properties

| Property                               | Description                                                                                                                        | Mandatory | Default                                 |
|--------------------------------------- |------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------------------------------------|
| kafka.topic                            | Kafka topic name to map the table to.                                                                                              | Yes       | null                                    |
| kafka.bootstrap.servers                | Table property indicating Kafka broker(s) connection string.                                                                       | Yes       | null                                    |
| kafka.serde.class                      | Serializer and Deserializer class implementation.                                                                                  | No        | org.apache.hadoop.hive.serde2.JsonSerDe |
| hive.kafka.poll.timeout.ms             | Parameter indicating Kafka Consumer poll timeout period in millis.  FYI this is independent from internal Kafka consumer timeouts. | No        | 5000 (5 Seconds)                        |
| hive.kafka.max.retries                 | Number of retries for Kafka metadata fetch operations.                                                                             | No        | 6                                       |
| hive.kafka.metadata.poll.timeout.ms    | Number of milliseconds before consumer timeout on fetching Kafka metadata.                                                         | No        | 30000 (30 Seconds)                      |
| kafka.write.semantic                   | Writer semantics, allowed values (AT_LEAST_ONCE, EXACTLY_ONCE)                                                                     | No        | AT_LEAST_ONCE                           |
| hive.kafka.ssl.credential.keystore     | Location of credential store that holds SSL credentials. Used to avoid plaintext passwords in table properties                     | No        |                                         |
| hive.kafka.ssl.truststore.password     | The key in the credential store used to retrieve the truststore password. This is NOT the password itself.                         | No        |                                         |
| hive.kafka.ssl.keystore.password       | The key in the credential store used to retrieve the keystore password. This is NOT the password itself. Used for 2-way auth.      | No        |                                         |
| hive.kafka.ssl.key.password            | The key in the credential store used to retrieve the key password. This is NOT the password itself.                                | No        |                                         |
| hive.kafka.ssl.truststore.location     | The location of the SSL truststore. Requires HDFS for queries that require jobs. Kafka requires this to be local, so pull it down. | No        |                                         |
| hive.kafka.ssl.keystore.location       | The location of the SSL keystore. Requires HDFS for queries that require jobs. Kafka requires this to be local, so pull it down.   | No        |                                         |

### SSL
The user can create SSL connections to Kafka, via the properties described in the table properties.
These properties are used to retrieve passwords from a credential store to avoid being in plaintext table properties.
To ensure security, the credential store should have appropriate permissions applied. Clients that query the table without
being able to read the credentials store will have the query fail.

Normally, the `<consumer/producer>.ssl.truststore.location` and `<consumer/producer>.ssl.keystore.location` would have to be local. Any job that requires a
job can retrieve these from an HDFS location, and they will be sourced from HDFS and pulled locally for this purpose.

The producer and consumer stores are both sourced from the same property, e.g. `hive.kafka.ssl.truststore.location`, rather than `kafka.consumer.ssl.truststore.location`.

#### SSL Example
Table creation is very simple, simply create a table as normal, and supply the appropriate Kafka
configs (e.g. `kafka.consumer.security.protocol`), along with the credential store configs (e.g. `hive.kafka.ssl.credential.store`).

```
CREATE EXTERNAL TABLE 
  kafka_ssl (
    `data` STRING
)
STORED BY 
  'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES ( 
  "kafka.topic" = "test-topic",
  "kafka.bootstrap.servers" = 'localhost:9093',
   'hive.kafka.ssl.credential.keystore'='jceks://hdfs/tmp/test.jceks',
   'hive.kafka.ssl.keystore.password'='keystore.password',
   'hive.kafka.ssl.truststore.password'='truststore.password',
   'kafka.consumer.security.protocol'='SSL',
   'hive.kafka.ssl.keystore.location'='hdfs://cluster/tmp/keystore.jks',
   'hive.kafka.ssl.truststore.location'='hdfs://cluster/tmp/keystore.jks'
);
```

Now we can query the table as normal.
```
SELECT * FROM kafka_ssl LIMIT 10;
```

Our truststore and keystore are located in HDFS, which means we can also run more complex queries that result in jobs.
These will still connect from Kafka as expected.
```
SELECT `data` FROM kafka_ssl where `__offset` > 0 AND `__offset` < 10000 group by `data`;

```



### Setting Extra Consumer/Producer properties.
The user can inject custom Kafka consumer/producer properties via the table properties.
To do so, the user can add any key/value pair of Kafka config to the Hive table property
by prefixing the key with `kafka.consumer` for consumer configs and `kafka.producer` for producer configs.
For instance the following alter table query adds the table property `"kafka.consumer.max.poll.records" = "5000"` 
and will inject `max.poll.records=5000` to the Kafka Consumer.

```sql
ALTER TABLE 
  kafka_table 
SET TBLPROPERTIES 
  ("kafka.consumer.max.poll.records" = "5000");
```

# Kafka to Hive ETL Pipeline Example

In this example we will load topic data only once.  The goal is to read data and commit both data and 
offsets in a single Transaction 

First, create the offset table.

```sql
DROP TABLE 
  kafka_table_offsets;
  
CREATE TABLE 
  kafka_table_offsets (
  `partition_id` INT,
  `max_offset` BIGINT,
  `insert_time` TIMESTAMP);
``` 

Initialize the table

```sql
INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
 `__partition`, 
 MIN(`__offset`) - 1, 
 CURRENT_TIMESTAMP 
FROM 
  wiki_kafka_hive 
GROUP BY 
  `__partition`, 
  CURRENT_TIMESTAMP;
``` 

Create the final Hive table for warehouse use,

```sql
DROP TABLE 
  orc_kafka_table;
  
CREATE TABLE 
  orc_kafka_table (
  `partition_id` INT,
  `koffset` BIGINT,
  `ktimestamp` BIGINT,
  `timestamp` TIMESTAMP,
  `page` STRING,
  `user` STRING,
  `diffurl` STRING,
  `isrobot` BOOLEAN,
  `added` INT,
  `deleted` INT,
  `delta` BIGINT) 
STORED AS ORC;
```

This is an example that inserts up to offset = 2 only.

```sql
FROM
  wiki_kafka_hive ktable 
JOIN 
  kafka_table_offsets offset_table
ON (
    ktable.`__partition` = offset_table.`partition_id`
  AND 
    ktable.`__offset` > offset_table.`max_offset` 
  AND  
    ktable.`__offset` < 3 )
    
INSERT INTO TABLE 
  orc_kafka_table 
SELECT 
  `__partition`,
  `__offset`,
  `__timestamp`,
  `timestamp`, 
  `page`, 
  `user`, 
  `diffurl`, 
  `isrobot`,
  `added`, 
  `deleted`,
  `delta`
  
INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
  `__partition`,
  max(`__offset`),
  CURRENT_TIMESTAMP
GROUP BY
  `__partition`, 
  CURRENT_TIMESTAMP;
```

Double check the insert.

```sql
SELECT
  max(`koffset`) 
FROM
  orc_kafka_table 
LIMIT 10;

SELECT
  COUNT(*) AS `c`  
FROM
  orc_kafka_table
GROUP BY
  `partition_id`,
  `koffset` 
HAVING 
  `c` > 1;
```

Conduct this as data becomes available on the topic. 

```sql
FROM
  wiki_kafka_hive ktable 
JOIN 
  kafka_table_offsets offset_table
ON ( 
    ktable.`__partition` = offset_table.`partition_id`
  AND 
    ktable.`__offset` > `offset_table.max_offset`)
    
INSERT INTO TABLE 
  orc_kafka_table 
SELECT 
  `__partition`, 
  `__offset`, 
  `__timestamp`,
  `timestamp`, 
  `page`, 
  `user`, 
  `diffurl`, 
  `isrobot`, 
  `added`, 
  `deleted`, 
  `delta`
  
INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
  `__partition`, 
  max(`__offset`), 
  CURRENT_TIMESTAMP 
GROUP BY 
  `__partition`, 
  CURRENT_TIMESTAMP;
```

# ETL from Hive to Kafka

## Kafka topic append with INSERT
First create the table in Hive that will be the target table. Now all the inserts will go to the topic mapped by 
this table.  Be aware that the Avro SerDe used below is regular Apache Avro (with schema) and not Confluent serialized
Avro which is popular with Kafka usage

```sql
CREATE EXTERNAL TABLE 
  moving_avg_wiki_kafka_hive ( 
    `channel` STRING, 
    `namespace` STRING,
    `page` STRING,
    `timestamp` TIMESTAMP, 
    `avg_delta` DOUBLE)
STORED BY 
  'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES(
  "kafka.topic" = "moving_avg_wiki_kafka_hive_2",
  "kafka.bootstrap.servers"="localhost:9092",
  -- STORE AS AVRO IN KAFKA
  "kafka.serde.class"="org.apache.hadoop.hive.serde2.avro.AvroSerDe");
```

Then, insert data into the table. Keep in mind that Kafka is append only thus you can not use insert overwrite. 

```sql
INSERT INTO TABLE
  moving_avg_wiki_kafka_hive 
SELECT 
  `channel`, 
  `namespace`, 
  `page`, 
  `timestamp`, 
  avg(`delta`) OVER (ORDER BY `timestamp` ASC ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS `avg_delta`, 
  null AS `__key`, 
  null AS `__partition`, 
  -1, 
  -1 
FROM 
  l15min_wiki;
```
