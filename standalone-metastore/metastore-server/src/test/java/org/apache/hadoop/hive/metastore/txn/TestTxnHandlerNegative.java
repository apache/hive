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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestTxnHandlerNegative {

  /**
   * This intentionally sets a bad URL for connection to test error handling
   * logic in TxnHandler.
   */
  @Test
  public void testBadConnection() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, "blah");

    final RuntimeException re = Assert.assertThrows(RuntimeException.class, () -> {
      TxnUtils.getTxnStore(conf);
    });

    Assert.assertEquals("Unable to instantiate raw store directly in fastpath mode", re.getMessage());
  }
}
