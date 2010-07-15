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

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;


public class TestHiveMetaStoreRemote extends TestCase {
  private static final String METASTORE_PORT = "29083";
private HiveMetaStoreClient client;
  private HiveConf hiveConf;
  boolean isServerRunning = false;

  private static class RunMS implements Runnable {

      @Override
      public void run() {
        System.out.println("Running metastore!");
        String [] args = new String [1];
        args[0] = METASTORE_PORT;
        HiveMetaStore.main(args);
      }

    }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    if(isServerRunning) {
      return;
    }
    Thread t = new Thread(new RunMS());
    t.start();

    // Wait a little bit for the metastore to start. Should probably have
    // a better way of detecting if the metastore has started?
    Thread.sleep(5000);

    // Set conf to connect to the local metastore.
    hiveConf = new HiveConf(this.getClass());
    // hive.metastore.local should be defined in HiveConf
    hiveConf.set("hive.metastore.local", "false");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + METASTORE_PORT);
    hiveConf.setIntVar(HiveConf.ConfVars.METATORETHRIFTRETRIES, 3);

    client = new HiveMetaStoreClient(hiveConf);
    // Now you have the client - run necessary tests.
    isServerRunning = true;
  }

  /**
   * tests create table and partition and tries to drop the table without
   * droppping the partition
   *
   * @throws Exception
   */
  public void testPartition() throws Exception {
    TestHiveMetaStore.partitionTester(client, hiveConf, true);
  }

}
