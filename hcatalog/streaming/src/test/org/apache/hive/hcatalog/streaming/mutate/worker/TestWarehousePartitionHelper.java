/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

public class TestWarehousePartitionHelper {

  private static final HiveConf CONFIGURATION = new HiveConf();
  private static final Path TABLE_PATH = new Path("table");
  
  private static final List<String> UNPARTITIONED_COLUMNS = Collections.emptyList();
  private static final List<String> UNPARTITIONED_VALUES = Collections.emptyList();
  
  private static final List<String> PARTITIONED_COLUMNS = Arrays.asList("A", "B");
  private static final List<String> PARTITIONED_VALUES = Arrays.asList("1", "2");
  
  private final PartitionHelper unpartitionedHelper;
  private final PartitionHelper partitionedHelper;

  public TestWarehousePartitionHelper() throws Exception {
    unpartitionedHelper = new WarehousePartitionHelper(CONFIGURATION, TABLE_PATH, UNPARTITIONED_COLUMNS);
    partitionedHelper = new WarehousePartitionHelper(CONFIGURATION, TABLE_PATH, PARTITIONED_COLUMNS);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void createNotSupported() throws Exception {
    unpartitionedHelper.createPartitionIfNotExists(UNPARTITIONED_VALUES);
  }

  @Test
  public void getPathForUnpartitionedTable() throws Exception {
    Path path = unpartitionedHelper.getPathForPartition(UNPARTITIONED_VALUES);
    assertThat(path, is(TABLE_PATH));
  }

  @Test
  public void getPathForPartitionedTable() throws Exception {
    Path path = partitionedHelper.getPathForPartition(PARTITIONED_VALUES);
    assertThat(path, is(new Path(TABLE_PATH, "A=1/B=2")));
  }

  @Test
  public void closeSucceeds() throws IOException {
    partitionedHelper.close();
    unpartitionedHelper.close();
  }
  
}
