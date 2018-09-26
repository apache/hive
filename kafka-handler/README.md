#Kafka Storage Handler Module

Storage Handler that allows user to Connect/Analyse/Transform Kafka topics.
The workflow is as follow,  first the user will create an external table that is a view over one Kafka topic,
then the user will be able to run any SQL query including write back to the same table or different kafka backed table.

## Usage

### Create Table
Use following statement to create table:
```sql
CREATE EXTERNAL TABLE kafka_table
(`timestamp` timestamp , `page` string,  `newPage` boolean,
 added int, deleted bigint, delta double)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "test-topic", "kafka.bootstrap.servers"="localhost:9092");
```
Table property `kafka.topic` is the Kafka Topic to connect to and `kafka.bootstrap.servers` is the Broker connection string.
Both properties are mandatory.
On the write path if such a topic does not exists the topic will be created if Kafka broker admin policy allow such operation.

By default the serializer and deserializer is Json `org.apache.hadoop.hive.serde2.JsonSerDe`.
If you want to switch serializer/deserializer classes you can use alter table.
```sql
ALTER TABLE kafka_table SET TBLPROPERTIES ("kafka.serde.class"="org.apache.hadoop.hive.serde2.avro.AvroSerDe");
``` 
List of supported Serializer Deserializer:

|Supported Serializer Deserializer|
|-----|
|org.apache.hadoop.hive.serde2.JsonSerDe|
|org.apache.hadoop.hive.serde2.OpenCSVSerde|
|org.apache.hadoop.hive.serde2.avro.AvroSerDe|
|org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe|
|org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe|

#### Table definition 
In addition to the user defined payload schema Kafka Storage Handler will append additional columns allowing user to query the Kafka metadata fields:
- `__key` Kafka record key (byte array)
- `__partition` Kafka record partition identifier (int 32)
- `__offset` Kafka record offset (int 64)
- `__timestamp` Kafka record timestamp (int 64)

 
### Query Table

List the table properties and all the partition/offsets information for the topic. 
```sql
Describe extended kafka_table;
```

Count the number of records with Kafka record timestamp within the last 10 minutes interval.

```sql
SELECT count(*) from kafka_table 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '10' MINUTES);
```
The storage handler allow filter push-down read optimization,
for instance the query above will only read the records with timestamp satisfying the filter predicate. 
Please note that such time based seek is only viable if the Kafka broker allow time based lookup (Kafka 0.11 or later versions)

In addition to **time based seek**, the storage handler reader is able to seek to a particular partition offset using the SQL WHERE clause.
Currently only support OR/AND with (<, <=, >=, >)

```sql
SELECT count(*)  from kafka_table
where (`__offset` < 10 and `__offset`>3 and `__partition` = 0)
or (`__partition` = 0 and `__offset` < 105 and `__offset` > 99)
or (`__offset` = 109);
```

User can define a view to take of the last 15 minutes and mask what ever column as follow:

```sql
CREATE VIEW last_15_minutes_of_kafka_table as select  `timestamp`, `user`, delta, added from kafka_table 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '15' MINUTES);
```

Join the Kafka Stream to Hive table. For instance assume you want to join the last 15 minutes of stream to dimension table like the following.
```sql
CREATE TABLE user_table (`user` string, `first_name` string , age int, gender string, comments string) STORED as ORC ;
```

Join the view of the last 15 minutes to `user_table`, group by user gender column and compute aggregates
over metrics from fact table and dimension table.

```sql
SELECT sum(added) as added, sum(deleted) as deleted, avg(delta) as delta, avg(age) as avg_age , gender 
FROM last_15_minutes_of_kafka_table  join user_table on `last_15_minutes_of_kafka_table`.`user` = `user_table`.`user`
GROUP BY gender limit 10;
```


Join the Stream to the Stream it self. In cases where you want to perform some Ad-Hoc query over the last 15 minutes view.
In the following example we show how you can perform classical user retention analysis over the Kafka Stream.
```sql
-- Steam join over the view it self
-- The example is adapted from https://www.periscopedata.com/blog/how-to-calculate-cohort-retention-in-sql
-- Assuming l15min_wiki is a view of the last 15 minutes
select  count( distinct activity.`user`) as active_users, count(distinct future_activity.`user`) as retained_users
from l15min_wiki as activity
left join l15min_wiki as future_activity on
  activity.`user` = future_activity.`user`
  and activity.`timestamp` = future_activity.`timestamp` - interval '5' minutes ;

--  Stream to stream join
-- Assuming wiki_kafka_hive is the entire stream.
select floor_hour(activity.`timestamp`), count( distinct activity.`user`) as active_users, count(distinct future_activity.`user`) as retained_users
from wiki_kafka_hive as activity
left join wiki_kafka_hive as future_activity on
  activity.`user` = future_activity.`user`
  and activity.`timestamp` = future_activity.`timestamp` - interval '1' hour group by floor_hour(activity.`timestamp`); 

```

#Configuration

## Table Properties

| Property                            | Description                                                                                                                        | Mandatory | Default                                 |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------------------------------------|
| kafka.topic                         | Kafka topic name to map the table to.                                                                                              | Yes       | null                                    |
| kafka.bootstrap.servers             | Table property indicating Kafka broker(s) connection string.                                                                       | Yes       | null                                    |
| kafka.serde.class                   | Serializer and Deserializer class implementation.                                                                                  | No        | org.apache.hadoop.hive.serde2.JsonSerDe |
| hive.kafka.poll.timeout.ms          | Parameter indicating Kafka Consumer poll timeout period in millis.  FYI this is independent from internal Kafka consumer timeouts. | No        | 5000 (5 Seconds)                        |
| hive.kafka.max.retries              | Number of retries for Kafka metadata fetch operations.                                                                             | No        | 6                                       |
| hive.kafka.metadata.poll.timeout.ms | Number of milliseconds before consumer timeout on fetching Kafka metadata.                                                         | No        | 30000 (30 Seconds)                      |
| kafka.write.semantic                | Writer semantic, allowed values (BEST_EFFORT, AT_LEAST_ONCE, EXACTLY_ONCE)                                                         | No        | AT_LEAST_ONCE                           |
| hive.kafka.optimistic.commit        | Boolean value indicate the if the producer should commit during task or delegate the commit to HS2.                                | No        | true                                    |

### Setting Extra Consumer/Producer properties.
User can inject custom Kafka consumer/producer properties via the Table properties.
To do so user can add any key/value pair of Kafka config to the Hive table property
by prefixing the key with `kafka.consumer` for consumer configs and `kafka.producer` for producer configs.
For instance the following alter table query adds the table property `"kafka.consumer.max.poll.records" = "5000"` 
and will inject `max.poll.records=5000` to the Kafka Consumer.
```sql
ALTER TABLE kafka_table SET TBLPROPERTIES ("kafka.consumer.max.poll.records"="5000");
```

#Kafka to Hive ETL PIPE LINE

load form Kafka every Record exactly once
Goal is to read data and commit both data and its offsets in a single Transaction 

First create the offset table.
```sql
Drop table kafka_table_offsets;
create table kafka_table_offsets(partition_id int, max_offset bigint, insert_time timestamp);
``` 

Initialize the table
```sql
insert overwrite table kafka_table_offsets select `__partition`, min(`__offset`) - 1, CURRENT_TIMESTAMP 
from wiki_kafka_hive group by `__partition`, CURRENT_TIMESTAMP ;
``` 
Create the end target table on the Hive warehouse.
```sql
Drop table orc_kafka_table;
Create table orc_kafka_table (partition_id int, koffset bigint, ktimestamp bigint,
 `timestamp` timestamp , `page` string, `user` string, `diffurl` string,
 `isrobot` boolean, added int, deleted int, delta bigint
) stored as ORC;
```
This an example tp insert up to offset = 2 only

```sql
From wiki_kafka_hive ktable JOIN kafka_table_offsets offset_table
on (ktable.`__partition` = offset_table.partition_id
and ktable.`__offset` > offset_table.max_offset and  ktable.`__offset` < 3 )
insert into table orc_kafka_table select `__partition`, `__offset`, `__timestamp`,
`timestamp`, `page`, `user`, `diffurl`, `isrobot`, added , deleted , delta
Insert overwrite table kafka_table_offsets select
`__partition`, max(`__offset`), CURRENT_TIMESTAMP group by `__partition`, CURRENT_TIMESTAMP;
```

Double check the insert
```sql
select max(`koffset`) from orc_kafka_table limit 10;
select count(*) as c  from orc_kafka_table group by partition_id, koffset having c > 1;
```

Repeat this periodically to insert all data.

```sql
From wiki_kafka_hive ktable JOIN kafka_table_offsets offset_table
on (ktable.`__partition` = offset_table.partition_id
and ktable.`__offset` > offset_table.max_offset )
insert into table orc_kafka_table select `__partition`, `__offset`, `__timestamp`,
`timestamp`, `page`, `user`, `diffurl`, `isrobot`, added , deleted , delta
Insert overwrite table kafka_table_offsets select
`__partition`, max(`__offset`), CURRENT_TIMESTAMP group by `__partition`, CURRENT_TIMESTAMP;
```

#ETL from Hive to Kafka

##INSERT INTO
First create the table in have that will be the target table. Now all the inserts will go to the topic mapped by this Table.

```sql
CREATE EXTERNAL TABLE moving_avg_wiki_kafka_hive
(`channel` string, `namespace` string,`page` string, `timestamp` timestamp , avg_delta double )
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "moving_avg_wiki_kafka_hive_2",
"kafka.bootstrap.servers"="cn105-10.l42scl.hortonworks.com:9092",
-- STORE AS AVRO IN KAFKA
"kafka.serde.class"="org.apache.hadoop.hive.serde2.avro.AvroSerDe");
```

Then insert data into the table. Keep in mind that Kafka is an append only, thus you can not use insert overwrite. 
```sql
insert into table moving_avg_wiki_kafka_hive select `channel`, `namespace`, `page`, `timestamp`, 
avg(delta) over (order by `timestamp` asc rows between  60 preceding and current row) as avg_delta, 
null as `__key`, null as `__partition`, -1, -1,-1, -1 from l15min_wiki;
```
