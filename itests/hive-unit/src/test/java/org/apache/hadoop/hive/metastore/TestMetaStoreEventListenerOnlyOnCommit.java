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

import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * Ensure that the status of MetaStore events depend on the RawStore's commit status.
 */
public class TestMetaStoreEventListenerOnlyOnCommit extends TestCase {

  private HiveConf hiveConf;
  private HiveMetaStoreClient msc;
  private Driver driver;

  @Override
  protected void setUp() throws Exception {

    super.setUp();

    DummyRawStoreControlledCommit.setCommitSucceed(true);

    System.setProperty(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname,
            DummyListener.class.getName());
    System.setProperty(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
            DummyRawStoreControlledCommit.class.getName());

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
    driver = new Driver(hiveConf);

    DummyListener.notifyList.clear();
  }

  public void testEventStatus() throws Exception {
    int listSize = 0;
    List<ListenerEvent> notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);

    driver.run("CREATE DATABASE tmpDb");
    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertTrue(DummyListener.getLastEvent().getStatus());

    driver.run("CREATE TABLE unittest_TestMetaStoreEventListenerOnlyOnCommit (id INT) " +
                "PARTITIONED BY (ds STRING)");
    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertTrue(DummyListener.getLastEvent().getStatus());

    driver.run("ALTER TABLE unittest_TestMetaStoreEventListenerOnlyOnCommit " +
                "ADD PARTITION(ds='foo1')");
    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertTrue(DummyListener.getLastEvent().getStatus());

    DummyRawStoreControlledCommit.setCommitSucceed(false);

    driver.run("ALTER TABLE unittest_TestMetaStoreEventListenerOnlyOnCommit " +
                "ADD PARTITION(ds='foo2')");
    listSize += 1;
    notifyList = DummyListener.notifyList;
    assertEquals(notifyList.size(), listSize);
    assertFalse(DummyListener.getLastEvent().getStatus());

  }
}
