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

import org.apache.hadoop.hive.conf.HiveConf;


public class TestRemoteHiveMetaStore extends TestHiveMetaStore {
  private static final String METASTORE_PORT = "29083";
  private static boolean isServerRunning = false;

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

    // hive.metastore.local should be defined in HiveConf
    hiveConf.set("hive.metastore.local", "false");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + METASTORE_PORT);
    hiveConf.setIntVar(HiveConf.ConfVars.METATORETHRIFTRETRIES, 3);

    client = new HiveMetaStoreClient(hiveConf);
    isThriftClient = true;

    // Now you have the client - run necessary tests.
    isServerRunning = true;
  }

}
