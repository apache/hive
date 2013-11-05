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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.mortbay.log.Log;

/**
 * TestHiveMetaStoreWithEnvironmentContext. Test case for _with_environment_context
 * calls in {@link org.apache.hadoop.hive.metastore.HiveMetaStore}
 */
public class TestHiveMetaStoreWithEnvironmentContext extends TestCase {

  private HiveConf hiveConf;
  private HiveMetaStoreClient msc;
  private EnvironmentContext envContext;
  private final Database db = new Database();
  private Table table = new Table();
  private final Partition partition = new Partition();

  private static final String dbName = "hive3252";
  private static final String tblName = "tmptbl";
  private static final String renamed = "tmptbl2";

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
    msc = new HiveMetaStoreClient(hiveConf, null);

    msc.dropDatabase(dbName, true, true);

    Map<String, String> envProperties = new HashMap<String, String>();
    envProperties.put("hadoop.job.ugi", "test_user");
    envContext = new EnvironmentContext(envProperties);

    db.setName(dbName);

    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put("a", "string");
    List<FieldSchema> partitionKeys = new ArrayList<FieldSchema>();
    partitionKeys.add(new FieldSchema("b", "string", ""));

    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("a", "string", ""));
    cols.add(new FieldSchema("b", "string", ""));
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setParameters(tableParams);
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tblName);
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");

    table.setDbName(dbName);
    table.setTableName(tblName);
    table.setParameters(tableParams);
    table.setPartitionKeys(partitionKeys);
    table.setSd(sd);

    List<String> partValues = new ArrayList<String>();
    partValues.add("2011");
    partition.setDbName(dbName);
    partition.setTableName(tblName);
    partition.setValues(partValues);
    partition.setSd(table.getSd().deepCopy());
    partition.getSd().setSerdeInfo(table.getSd().getSerdeInfo().deepCopy());

    DummyListener.notifyList.clear();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testEnvironmentContext() throws Exception {
    int listSize = 0;

    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    msc.createDatabase(db);
    listSize++;
    assertEquals(listSize, notifyList.size());
    CreateDatabaseEvent dbEvent = (CreateDatabaseEvent)(notifyList.get(listSize - 1));
    assert dbEvent.getStatus();

    Log.debug("Creating table");
    msc.createTable(table, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    CreateTableEvent tblEvent = (CreateTableEvent)(notifyList.get(listSize - 1));
    assert tblEvent.getStatus();
    assertEquals(envContext, tblEvent.getEnvironmentContext());

    table = msc.getTable(dbName, tblName);

    Log.debug("Adding partition");
    partition.getSd().setLocation(table.getSd().getLocation() + "/part1");
    msc.add_partition(partition, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    AddPartitionEvent partEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assert partEvent.getStatus();
    assertEquals(envContext, partEvent.getEnvironmentContext());

    Log.debug("Appending partition");
    List<String> partVals = new ArrayList<String>();
    partVals.add("2012");
    msc.appendPartition(dbName, tblName, partVals, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    AddPartitionEvent appendPartEvent = (AddPartitionEvent)(notifyList.get(listSize-1));
    assert appendPartEvent.getStatus();
    assertEquals(envContext, appendPartEvent.getEnvironmentContext());

    Log.debug("Renaming table");
    table.setTableName(renamed);
    msc.alter_table(dbName, tblName, table, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    AlterTableEvent alterTableEvent = (AlterTableEvent) notifyList.get(listSize-1);
    assert alterTableEvent.getStatus();
    assertEquals(envContext, alterTableEvent.getEnvironmentContext());

    Log.debug("Renaming table back");
    table.setTableName(tblName);
    msc.alter_table(dbName, renamed, table, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);

    Log.debug("Dropping partition");
    List<String> dropPartVals = new ArrayList<String>();
    dropPartVals.add("2011");
    msc.dropPartition(dbName, tblName, dropPartVals, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropPartitionEvent dropPartEvent = (DropPartitionEvent)notifyList.get(listSize - 1);
    assert dropPartEvent.getStatus();
    assertEquals(envContext, dropPartEvent.getEnvironmentContext());

    Log.debug("Dropping partition by name");
    msc.dropPartition(dbName, tblName, "b=2012", true, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropPartitionEvent dropPartByNameEvent = (DropPartitionEvent)notifyList.get(listSize - 1);
    assert dropPartByNameEvent.getStatus();
    assertEquals(envContext, dropPartByNameEvent.getEnvironmentContext());

    Log.debug("Dropping table");
    msc.dropTable(dbName, tblName, true, false, envContext);
    listSize++;
    assertEquals(notifyList.size(), listSize);
    DropTableEvent dropTblEvent = (DropTableEvent)notifyList.get(listSize-1);
    assert dropTblEvent.getStatus();
    assertEquals(envContext, dropTblEvent.getEnvironmentContext());

    msc.dropDatabase(dbName);
    listSize++;
    assertEquals(notifyList.size(), listSize);

    DropDatabaseEvent dropDB = (DropDatabaseEvent)notifyList.get(listSize-1);
    assert dropDB.getStatus();
  }

}
