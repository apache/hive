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

package org.apache.hadoop.hive.metastore;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * TestRetryingHMSHandler. Test case for
 * {@link org.apache.hadoop.hive.metastore.RetryingHMSHandler}
 */
@Category(MetastoreCheckinTest.class)
public class TestRetryingHMSHandler {
  private Configuration conf;
  private HiveMetaStoreClient msc;

  @Before
  public void setUp() throws Exception {
    System.setProperty("hive.metastore.pre.event.listeners",
        AlternateFailurePreListener.class.getName());
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setLongVar(conf, ConfVars.HMSHANDLERATTEMPTS, 2);
    MetastoreConf.setTimeVar(conf, ConfVars.HMSHANDLERINTERVAL, 0, TimeUnit.MILLISECONDS);
    MetastoreConf.setBoolVar(conf, ConfVars.HMSHANDLERFORCERELOADCONF, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    msc = new HiveMetaStoreClient(conf);
  }

  // Create a database and a table in that database.  Because the AlternateFailurePreListener is
  // being used each attempt to create something should require two calls by the RetryingHMSHandler
  @Test
  public void testRetryingHMSHandler() throws Exception {
    String dbName = "hive4159";
    String tblName = "tmptbl";

    Database db = new Database();
    db.setName(dbName);
    msc.createDatabase(db);

    Assert.assertEquals(2, AlternateFailurePreListener.getCallCount());

    Table tbl = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .addCol("c1", ColumnType.STRING_TYPE_NAME)
        .build();

    msc.createTable(tbl);

    Assert.assertEquals(4, AlternateFailurePreListener.getCallCount());
  }

}
