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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.junit.Test;

public class TestHCatExternalDynamicPartitioned extends TestHCatDynamicPartitioned {

  public TestHCatExternalDynamicPartitioned(String formatName, String serdeClass,
      String inputFormatClass, String outputFormatClass)
      throws Exception {
    super(formatName, serdeClass, inputFormatClass, outputFormatClass);
    tableName = "testHCatExternalDynamicPartitionedTable_" + formatName;
    generateWriteRecords(NUM_RECORDS, NUM_PARTITIONS, 0);
    generateDataColumns();
  }

  @Override
  protected Boolean isTableExternal() {
    return true;
  }

  /**
   * Run the external dynamic partitioning test but with single map task
   * @throws Exception
   */
  @Test
  public void testHCatExternalDynamicCustomLocation() throws Exception {
    runHCatDynamicPartitionedTable(true, "mapred/externalDynamicOutput/${p1}");
  }

}
