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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.llap.io.encoded;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Without the IO cache the native Parquet reader has no footer cache, so LLAP IO declines Parquet and the split
 * takes the stock reader.
 */
public class TestParquetNativeIOCacheDisabled {

  @BeforeClass
  public static void setUpClass() throws Exception {
    HiveConf daemonConf = new HiveConf();
    HiveConf.setVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MODE, "none");
    HiveConf.setBoolVar(daemonConf, ConfVars.LLAP_IO_PARQUET_NATIVE_ENABLED, true);
    LlapProxy.setDaemon(true);
    LlapProxy.initializeLlapIo(daemonConf);
    assertFalse(LlapProxy.getIo().usingLowLevelCache());
  }

  @AfterClass
  public static void tearDownClass() {
    LlapProxy.close();
  }

  @Test
  public void testNativeParquetIsDeclined() {
    assertNull(LlapProxy.getIo().getInputFormat(new MapredParquetInputFormat(), null));
  }
}
