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
package org.apache.hadoop.hive.metastore.txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class TestTxnHandlerNegative {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnHandlerNegative.class);

  /**
   * this intentionally sets a bad URL for connection to test error handling logic
   * in TxnHandler
   * @throws Exception
   */
  @Test
  public void testBadConnection() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "blah");
    RuntimeException e = null;
    try {
      TxnUtils.getTxnStore(conf);
    }
    catch(RuntimeException ex) {
      LOG.info("Expected error: " + ex.getMessage(), ex);
      e = ex;
    }
    assertNotNull(e);
    assertTrue(
        e.getMessage().contains("No suitable driver found for blah")
        || e.getMessage().contains("Failed to get driver instance for jdbcUrl=blah")
    );
  }
}
