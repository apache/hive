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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestGenMapRedUtilsUsePartitionColumnsPositive {

  @Parameterized.Parameters(name = "{index}: updatePartitions({2})")
  public static Iterable<Object[]> testCases() {
    return Arrays.asList(new Object[][]{
      {"p1/p2/p3","t1:t2:t3","p2","p2","t2"},
      {"p1/p2/p3","t1:t2:t3","p2,p3","p2/p3","t2:t3"},
      {"p1/p2/p3","t1:t2:t3","p1,p2,p3","p1/p2/p3","t1:t2:t3"},
      {"p1/p2/p3","t1:t2:t3","p1,p3","p1/p3","t1:t3"},
      {"p1","t1","p1","p1","t1"},
      {"p1/p2/p3","t1:t2:t3","p3,p2,p1","p3/p2/p1","t3:t2:t1"}
    });
  }

  @Parameterized.Parameter(0) public String inPartColNames;
  @Parameterized.Parameter(1) public String inPartColTypes;
  @Parameterized.Parameter(2) public String partNamesToRetain;
  @Parameterized.Parameter(3) public String expectedPartColNames;
  @Parameterized.Parameter(4) public String expectedPartColTypes;

  @Test
  public void testUsePartitionColumns() {
    Properties p = new Properties();
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, inPartColNames);
    p.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, inPartColTypes);
    GenMapRedUtils.usePartitionColumns(p, Arrays.asList(partNamesToRetain.split(",")));
    String actualNames = p.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    String actualTypes = p.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
    assertEquals(expectedPartColNames, actualNames);
    assertEquals(expectedPartColTypes, actualTypes);
  }
}
