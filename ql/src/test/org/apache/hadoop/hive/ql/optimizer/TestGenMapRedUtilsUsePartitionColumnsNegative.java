/**
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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.junit.Test;

public class TestGenMapRedUtilsUsePartitionColumnsNegative {

  @Test(expected = NullPointerException.class)
  public void testUsePartitionColumnsNoPartColNames() {
    Properties p = new Properties();
    GenMapRedUtils.usePartitionColumns(p, Arrays.asList("p1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUsePartitionColumnsNamesTypesMismatch() {
    Properties p = new Properties();
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "p1/p2");
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, "t1");
    GenMapRedUtils.usePartitionColumns(p, Arrays.asList("p1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUsePartitionColumnsNoPartitionsToRetain() {
    Properties p = new Properties();
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "p1");
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, "t1");
    GenMapRedUtils.usePartitionColumns(p, Collections.EMPTY_LIST);
  }

  @Test(expected = RuntimeException.class)
  public void testUsePartitionColumnsWrongPartColName() {
    Properties p = new Properties();
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "p1");
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, "t1");
    GenMapRedUtils.usePartitionColumns(p, Arrays.asList("p2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUsePartitionColumnsDuplicatePartColNameInArgument() {
    Properties p = new Properties();
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "p1/p2");
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, "t1:t2");
    GenMapRedUtils.usePartitionColumns(p, Arrays.asList("p1","p2","p1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUsePartitionColumnsDuplicatePartColNameInConfiguration() {
    Properties p = new Properties();
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "p1/p2/p1");
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, "t1:t2:t3");
    GenMapRedUtils.usePartitionColumns(p, Arrays.asList("p1"));
  }
}
