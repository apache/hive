/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link MRCompactor#createBaseJobConf(HiveConf, String, Table, StorageDescriptor, ValidWriteIdList, CompactionInfo)}.
 */
public class TestMRCompactorJobQueueConfiguration {

  @ParameterizedTest
  @MethodSource("generateBaseJobConfSetup")
  void testCreateBaseJobConfHasCorrectJobQueue(ConfSetup input) {
    Table tbl = createPersonTable();
    tbl.setParameters(input.tableProperties);
    MRCompactor compactor = new MRCompactor(null);
    CompactionInfo ci = new CompactionInfo(tbl.getDbName(), tbl.getTableName(), null, CompactionType.MAJOR);
    ci.properties = new StringableMap(input.compactionProperties).toString();
    HiveConf conf = new HiveConf();
    input.confProperties.forEach(conf::set);
    JobConf c = compactor.createBaseJobConf(conf, "test-job", tbl, tbl.getSd(), new ValidReaderWriteIdList(), ci);
    assertEquals(input.expectedQueue, c.getQueueName(), "Test failed for the following input:" + input);
  }

  private static Stream<ConfSetup> generateBaseJobConfSetup() {
    List<ConfSetup> inputs = new ArrayList<>();
    String mrProperty = "mapreduce.job.queuename";
    String hiveProperty = "hive.compactor.job.queue";
    String mrDeprecated = "mapred.job.queue.name";
    // Use u1 to u3 values for table properties
    String u1 = "root.user1";
    String u2 = "root.user2";
    String u3 = "root.user3";
    // Use u4 to u6 values for compaction properties
    String u4 = "root.user4";
    String u5 = "root.user5";
    String u6 = "root.user6";
    // Use su1 to su3 for for global properties
    String su1 = "superuser1";
    String su2 = "superuser2";
    String su3 = "superuser3";
    // Check precedence of queue properties when set per table
    // CREATE TABLE ... TBLPROPERTIES (...)
    inputs.add(new ConfSetup()
        .tableProperty(mrDeprecated, u1)
        .tableProperty(mrProperty, u2)
        .tableProperty(hiveProperty, u3)
        .setExpectedQueue(u3));
    inputs.add(new ConfSetup().tableProperty(mrDeprecated, u1).tableProperty(hiveProperty, u3).setExpectedQueue(u3));
    inputs.add(new ConfSetup().tableProperty(mrProperty, u2).tableProperty(hiveProperty, u3).setExpectedQueue(u3));
    inputs.add(new ConfSetup().tableProperty(mrDeprecated, u1).tableProperty(mrProperty, u2).setExpectedQueue(u2));
    inputs.add(new ConfSetup().tableProperty(mrDeprecated, u1).setExpectedQueue(u1));
    inputs.add(new ConfSetup().tableProperty(mrProperty, u2).setExpectedQueue(u2));
    inputs.add(new ConfSetup().tableProperty(hiveProperty, u3).setExpectedQueue(u3));
    // Check precedence of queue properties when set per compaction request
    // ALTER TABLE ... COMPACT ... TBLPROPERTIES (...)
    inputs.add(new ConfSetup()
        .compactionProperty(mrDeprecated, u4)
        .compactionProperty(mrProperty, u5)
        .compactionProperty(hiveProperty, u6)
        .setExpectedQueue(u6));
    inputs.add(new ConfSetup()
        .compactionProperty(mrDeprecated, u4)
        .compactionProperty(hiveProperty, u6)
        .setExpectedQueue(u6));
    inputs.add(new ConfSetup()
        .compactionProperty(mrProperty, u5)
        .compactionProperty(hiveProperty, u6)
        .setExpectedQueue(u6));
    inputs.add(new ConfSetup()
        .compactionProperty(mrDeprecated, u4)
        .compactionProperty(mrProperty, u5)
        .setExpectedQueue(u5));
    inputs.add(new ConfSetup().compactionProperty(mrDeprecated, u4).setExpectedQueue(u4));
    inputs.add(new ConfSetup().compactionProperty(mrProperty, u5).setExpectedQueue(u5));
    inputs.add(new ConfSetup().compactionProperty(hiveProperty, u6).setExpectedQueue(u6));
    // Check precedence of queue properties when set globally
    inputs.add(new ConfSetup().globalProperty(hiveProperty, su1).setExpectedQueue(su1));
    inputs.add(new ConfSetup().globalProperty(mrProperty, su2).setExpectedQueue(su2));
    inputs.add(new ConfSetup().globalProperty(mrDeprecated, su3).setExpectedQueue(su3));
    inputs.add(new ConfSetup()
        .globalProperty(hiveProperty, su1)
        .globalProperty(mrProperty, su2)
        .globalProperty(mrDeprecated, su3)
        .setExpectedQueue(su1));
    inputs.add(new ConfSetup()
        .globalProperty(mrProperty, su2)
        .globalProperty(mrDeprecated, su3)
        .setExpectedQueue(su2));
    inputs.add(new ConfSetup()
        .globalProperty(hiveProperty, su1)
        .globalProperty(mrDeprecated, su3)
        .setExpectedQueue(su1));
    inputs.add(new ConfSetup().globalProperty(hiveProperty, su1).globalProperty(mrProperty, su2).setExpectedQueue(su1));
    // Check precedence of queue properties when set per table, per compaction request, globally. The expected order is:
    // i)   compaction request, 
    // ii)  table properties, 
    // iii) global conf
    inputs.add(new ConfSetup()
        .tableProperty(hiveProperty, u3)
        .compactionProperty(hiveProperty, u6)
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u6));
    inputs.add(new ConfSetup()
        .tableProperty(hiveProperty, u3)
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u3));
    inputs.add(new ConfSetup()
        .compactionProperty(hiveProperty, u6)
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u6));
    inputs.add(new ConfSetup()
        .tableProperty(hiveProperty, u3)
        .compactionProperty(hiveProperty, u6)
        .setExpectedQueue(u6));
    // Check combination of MR properties at table/compaction level and Hive property globally.
    inputs.add(new ConfSetup()
        .tableProperty(mrProperty, u2)
        .compactionProperty(mrProperty, u5)
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u5));
    inputs.add(new ConfSetup()
        .tableProperty(mrDeprecated, u1)
        .compactionProperty(mrDeprecated, u4)
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u4));
    inputs.add(new ConfSetup()
        .tableProperty(mrProperty, u2)
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u2));
    inputs.add(new ConfSetup()
        .tableProperty(mrDeprecated, u1)
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u1));
    // Check empty properties are ignored
    inputs.add(new ConfSetup()
        .tableProperty(hiveProperty, u3)
        .compactionProperty(hiveProperty, "")
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(u3));
    inputs.add(new ConfSetup()
        .tableProperty(hiveProperty, "")
        .compactionProperty(hiveProperty, "")
        .globalProperty(hiveProperty, su1)
        .setExpectedQueue(su1));
    inputs.add(new ConfSetup()
        .tableProperty(hiveProperty, "")
        .compactionProperty(hiveProperty, "")
        .globalProperty(hiveProperty, "")
        .setExpectedQueue("default"));
    return inputs.stream();
  }

  /**
   * Creates a minimal table resembling a PERSON type (see DDL below) for testing purposes.
   *
   * <pre>{@code
   * CREATE TABLE default.person (id INT, name STRING) STORED AS TEXTFILE
   * }</pre>
   *
   * @return a new table representing a person.
   */
  private static Table createPersonTable() {
    FieldSchema idField = new FieldSchema();
    idField.setName("id");
    idField.setType("int");
    FieldSchema nameField = new FieldSchema();
    nameField.setName("name");
    nameField.setType("string");
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    descriptor.setOutputFormat("org.apache.hadoop.mapred.TextInputFormat");
    descriptor.setLocation("hdfs:///apps/hive/warehouse/default.db/person");
    descriptor.setCompressed(false);
    descriptor.setCols(Arrays.asList(idField, nameField));
    long createTime = LocalDate.of(2022, 1, 24).atStartOfDay().toInstant(ZoneOffset.UTC).getEpochSecond();
    Table tbl = new Table();
    tbl.setDbName("default");
    tbl.setTableName("person");
    tbl.setOwner("hive");
    tbl.setCreateTime(Math.toIntExact(createTime));
    tbl.setLastAccessTime(Math.toIntExact(createTime));
    tbl.setSd(descriptor);
    tbl.setParameters(Collections.emptyMap());
    return tbl;
  }

  /**
   * Class for constructing and keeping property configurations for testing the compactor's job queue.
   */
  private static class ConfSetup {
    private final Map<String, String> tableProperties = new HashMap<>();
    private final Map<String, String> compactionProperties = new HashMap<>();
    private final Map<String, String> confProperties = new HashMap<>();
    private String expectedQueue;

    ConfSetup tableProperty(String key, String value) {
      tableProperties.put("compactor." + key, value);
      return this;
    }

    ConfSetup compactionProperty(String key, String value) {
      compactionProperties.put("compactor." + key, value);
      return this;
    }

    ConfSetup globalProperty(String key, String value) {
      confProperties.put(key, value);
      return this;
    }

    ConfSetup setExpectedQueue(String name) {
      expectedQueue = name;
      return this;
    }

    @Override
    public String toString() {
      return "ConfSetup{" + "tableProperties=" + tableProperties + ", compactionProperties=" + compactionProperties
          + ", confProperties=" + confProperties + '}';
    }
  }
}
