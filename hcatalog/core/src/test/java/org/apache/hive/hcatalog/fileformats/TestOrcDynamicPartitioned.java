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

package org.apache.hive.hcatalog.fileformats;

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hive.hcatalog.mapreduce.TestHCatDynamicPartitioned;
import org.junit.BeforeClass;

public class TestOrcDynamicPartitioned extends TestHCatDynamicPartitioned {

  @BeforeClass
  public static void generateInputData() throws Exception {
    tableName = "testOrcDynamicPartitionedTable";
    generateWriteRecords(NUM_RECORDS, NUM_PARTITIONS, 0);
    generateDataColumns();
  }

  @Override
  protected String inputFormat() { 
    return OrcInputFormat.class.getName();
  }

  @Override
  protected String outputFormat() { 
    return OrcOutputFormat.class.getName(); 
  }

  @Override
  protected String serdeClass() { 
    return OrcSerde.class.getName(); 
  }

}
