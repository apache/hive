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
package org.apache.hive.hcatalog.pig;

import java.io.IOException;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParquetHCatLoader extends AbstractHCatLoaderTest {
  static Logger LOG = LoggerFactory.getLogger(TestParquetHCatLoader.class);
  @Override
  String getStorageFormat() {
    return IOConstants.PARQUET;
  }

  @Override
  @Test
  @Ignore("Temporarily disable until fixed")
  public void testReadDataBasic() throws IOException {
    super.testReadDataBasic();
  }

  @Override
  @Test
  @Ignore("Temporarily disable until fixed")
  public void testReadPartitionedBasic() throws IOException, CommandNeedRetryException {
    super.testReadPartitionedBasic();
  }

  @Override
  @Test
  @Ignore("Temporarily disable until fixed")
  public void testProjectionsBasic() throws IOException {
    super.testProjectionsBasic();
  }

  /**
   * Tests the failure case caused by HIVE-10752
   * @throws Exception
   */
  @Override
  @Test
  @Ignore("Temporarily disable until fixed")
  public void testColumnarStorePushdown2() throws Exception {
    super.testColumnarStorePushdown2();
  }

  @Override
  @Test
  @Ignore("Temporarily disable until fixed")
  public void testReadMissingPartitionBasicNeg() throws IOException, CommandNeedRetryException {
    super.testReadMissingPartitionBasicNeg();
  }

  /**
   * Test if we can read a date partitioned table
   */
  @Override
  @Test
  @Ignore("Temporarily disable until fixed")
  public void testDatePartitionPushUp() throws Exception {
    super.testDatePartitionPushUp();
  }
}
