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

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for TxnDbUtil
 */
public class TestTxnDbUtil {
  private HiveConf conf;

  public TestTxnDbUtil() throws Exception {
  }

  @Test
  public void testCountQueryAgent() throws Exception {
    int count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE in ('t10', 't11')");
    Assert.assertEquals(0, count);
  }

  @Test
  public void testQueryToString() throws Exception {
    Assert.assertEquals("CTC_TXNID   CTC_DATABASE   CTC_TABLE   CTC_PARTITION".trim(),TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS").trim());
  }

  @Before
  public void setUp() throws Exception {
    tearDown();
    conf = new HiveConf(this.getClass());
    TxnDbUtil.setConfValues(conf);
    TxnDbUtil.prepDbWithExternalConf(conf);
  }

  @After
  public void tearDown() throws Exception {
    TxnDbUtil.cleanDb();
  }
}
