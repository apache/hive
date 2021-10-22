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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Tests to ensure function works even with connection pool size is 1
 */
public class TestTxnHandlerWithOneConnection {
  static final private String CLASS_NAME = TxnHandler.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private HiveConf conf = new HiveConf();
  private TxnStore txnHandler;

  public TestTxnHandlerWithOneConnection() throws Exception {
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration conf = ctx.getConfiguration();
    conf.getLoggerConfig(CLASS_NAME).setLevel(Level.DEBUG);
    ctx.updateLoggers(conf);
    tearDown();
  }

  @Test
  public void testGetValidWriteIds() throws Exception {
    GetValidWriteIdsRequest req = new GetValidWriteIdsRequest();
    req.setFullTableNames(Collections.singletonList("foo.bar"));
    txnHandler.getValidWriteIds(req);
  }

  @Before
  public void setUp() throws Exception {
    // set the connection pool size to 1
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS, 1);
    // set the connection timeout to the minimum accepted value
    String CONNECTION_TIMEOUT_PROPERTY = "hikaricp.connectionTimeout";
    conf.setLong(CONNECTION_TIMEOUT_PROPERTY, 250L);
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @After
  public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(conf);
  }
}
