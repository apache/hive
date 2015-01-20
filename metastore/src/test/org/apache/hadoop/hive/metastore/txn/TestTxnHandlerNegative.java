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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Test;

public class TestTxnHandlerNegative {
  static final private Log LOG = LogFactory.getLog(TestTxnHandlerNegative.class);

  /**
   * this intentionally sets a bad URL for connection to test error handling logic
   * in TxnHandler
   * @throws Exception
   */
  @Test
  public void testBadConnection() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "blah");
    TxnHandler txnHandler1 = new TxnHandler(conf);
    MetaException e = null;
    try {
      txnHandler1.getOpenTxns();
    }
    catch(MetaException ex) {
      LOG.info("Expected error: " + ex.getMessage(), ex);
      e = ex;
    }
    assert e != null : "did not get exception";
  }
}
