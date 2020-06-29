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

package org.apache.hive.jdbc;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * TestNewGetSplitsFormatReturnPath.
 */
@Ignore("flaky HIVE-23524")
public class TestNewGetSplitsFormatReturnPath extends TestNewGetSplitsFormat {

  @BeforeClass public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(HiveConf.ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_FILESINK_ARROW_NATIVE_ENABLED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP, true);
    BaseJdbcWithMiniLlap.beforeTest(conf);
  }

  @Override
  @Ignore
  @Test
  public void testMultipleBatchesOfComplexTypes() {
    // ToDo: FixMe
  }

  @Override
  @Ignore("HIVE-23524 flaky")
  @Test
  public void testLlapInputFormatEndToEndWithMultipleBatches() {
  }
}
