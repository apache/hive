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
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * TestHiveMetaStoreStatsMerge.
 * calls in {@link org.apache.hadoop.hive.metastore.HiveMetaStore}
 */
public class TestHiveMetaStoreStatsMerge extends TestCase {

  private HiveConf hiveConf;
  private HiveMetaStoreClient msc;
  private final Database db = new Database();
  private Table table = new Table();

  private static final String dbName = "hive3253";
  private static final String tblName = "tmptbl";

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    System.setProperty("hive.metastore.event.listeners",
        DummyListener.class.getName());

    int port = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(new CliSessionState(hiveConf));
    msc = new HiveMetaStoreClient(hiveConf);

    msc.dropDatabase(dbName, true, true);

    db.setName(dbName);

    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put("a", "string");

    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("a", "string", ""));
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setParameters(tableParams);
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tblName);
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());

    table.setDbName(dbName);
    table.setTableName(tblName);
    table.setParameters(tableParams);
    table.setSd(sd);

    DummyListener.notifyList.clear();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testStatsMerge() throws Exception {
    int listSize = 0;

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    msc.createDatabase(db);
    listSize++;
    assertEquals(listSize, notifyList.size());
    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(listSize - 1));
    assert dbEvent.getStatus();

    msc.createTable(table);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(listSize - 1));
    assert tblEvent.getStatus();

    table = msc.getTable(dbName, tblName);

    ColumnStatistics cs = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, dbName, tblName);
    cs.setStatsDesc(desc);
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName("a");
    obj.setColType("string");
    ColumnStatisticsData data = new ColumnStatisticsData();
    StringColumnStatsData scsd = new StringColumnStatsData();
    scsd.setAvgColLen(10);
    scsd.setMaxColLen(20);
    scsd.setNumNulls(30);
    scsd.setNumDVs(123);
    scsd.setBitVectors("{0, 4, 5, 7}{0, 1}{0, 1, 2}{0, 1, 4}{0}{0, 2}{0, 3}{0, 2, 3, 4}{0, 1, 4}{0, 1}{0}{0, 1, 3, 8}{0, 2}{0, 2}{0, 9}{0, 1, 4}");
    data.setStringStats(scsd);
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    
    List<ColumnStatistics> colStats = new ArrayList<>();
    colStats.add(cs);
    
    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStats);
    msc.setPartitionColumnStatistics(request);

    List<String> colNames = new ArrayList<>();
    colNames.add("a");

    StringColumnStatsData getScsd = msc.getTableColumnStatistics(dbName, tblName, colNames).get(0)
        .getStatsData().getStringStats();
    assertEquals(getScsd.getNumDVs(), 123);
    
    cs = new ColumnStatistics();
    scsd = new StringColumnStatsData();
    scsd.setAvgColLen(20);
    scsd.setMaxColLen(5);
    scsd.setNumNulls(70);
    scsd.setNumDVs(456);
    scsd.setBitVectors("{0, 1}{0, 1}{1, 2, 4}{0, 1, 2}{0, 1, 2}{0, 2}{0, 1, 3, 4}{0, 1}{0, 1}{3, 4, 6}{2}{0, 1}{0, 3}{0}{0, 1}{0, 1, 4}");
    data.setStringStats(scsd);
    obj.setStatsData(data);
    cs.addToStatsObj(obj);
    
    request = new SetPartitionsStatsRequest(colStats);
    request.setNeedMerge(true);
    msc.setPartitionColumnStatistics(request);
    
    getScsd = msc.getTableColumnStatistics(dbName, tblName, colNames).get(0)
        .getStatsData().getStringStats();
    assertEquals(getScsd.getAvgColLen(), 20.0);
    assertEquals(getScsd.getMaxColLen(), 20);
    assertEquals(getScsd.getNumNulls(), 100);
    // since metastore is ObjectStore, we use the max function to merge.
    assertEquals(getScsd.getNumDVs(), 456);

  }

}
