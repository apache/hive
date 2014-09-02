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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * TestRetryingHMSHandler. Test case for
 * {@link org.apache.hadoop.hive.metastore.RetryingHMSHandler}
 */
public class TestRetryingHMSHandler extends TestCase {
  private HiveConf hiveConf;
  private HiveMetaStoreClient msc;

  @Override
  protected void setUp() throws Exception {

    super.setUp();
    System.setProperty("hive.metastore.pre.event.listeners",
        AlternateFailurePreListener.class.getName());
    int port = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());
    hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HMSHANDLERATTEMPTS, 2);
    hiveConf.setTimeVar(HiveConf.ConfVars.HMSHANDLERINTERVAL, 0, TimeUnit.MILLISECONDS);
    hiveConf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, false);
    msc = new HiveMetaStoreClient(hiveConf, null);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  // Create a database and a table in that database.  Because the AlternateFailurePreListener is
  // being used each attempt to create something should require two calls by the RetryingHMSHandler
  public void testRetryingHMSHandler() throws Exception {
    String dbName = "hive4159";
    String tblName = "tmptbl";

    Database db = new Database();
    db.setName(dbName);
    msc.createDatabase(db);

    Assert.assertEquals(2, AlternateFailurePreListener.getCallCount());

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

    Map<String, String> params = new HashMap<String, String>();
    params.put("test_param_1", "Use this for comments etc");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

    StorageDescriptor sd = new StorageDescriptor();

    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(params);
    sd.setBucketCols(new ArrayList<String>(2));
    sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tblName);
    sd.getSerdeInfo().setParameters(serdParams);
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());
    sd.setSortCols(new ArrayList<Order>());

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    tbl.setSd(sd);
    tbl.setLastAccessTime(0);

    msc.createTable(tbl);

    Assert.assertEquals(4, AlternateFailurePreListener.getCallCount());
  }

}
