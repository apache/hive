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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;


public class TestRemoteHiveMetaStore extends TestHiveMetaStore {
  protected static final String METASTORE_PORT = "29083";
  private static boolean isServerStarted = false;

  public TestRemoteHiveMetaStore() {
    super();
    isThriftClient = true;
  }

  private static class RunMS implements Runnable {

      @Override
      public void run() {
        try {
        HiveMetaStore.main(new String[] { METASTORE_PORT });
        } catch (Throwable e) {
          e.printStackTrace(System.err);
          assert false;
        }
      }

    }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    if (isServerStarted) {
      assertNotNull("Unable to connect to the MetaStore server", client);
      return;
    }

    System.out.println("Starting MetaStore Server on port " + METASTORE_PORT);
    Thread t = new Thread(new RunMS());
    t.start();
    isServerStarted = true;

    // Wait a little bit for the metastore to start. Should probably have
    // a better way of detecting if the metastore has started?
    Thread.sleep(5000);
    // This is default case with setugi off for both client and server
    createClient(false);
  }

  protected void createClient(boolean setugi) throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + METASTORE_PORT);
    hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI,setugi);
    client = new HiveMetaStoreClient(hiveConf);
  }
}
