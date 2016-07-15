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
import org.apache.hadoop.hive.shims.ShimLoader;


public class TestRemoteHiveMetaStore extends TestHiveMetaStore {
  private static boolean isServerStarted = false;
  private static int port;

  public TestRemoteHiveMetaStore() {
    super();
    isThriftClient = true;
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    if (isServerStarted) {
      assertNotNull("Unable to connect to the MetaStore server", client);
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
      return;
    }

    port = MetaStoreUtils.findFreePort();
    System.out.println("Starting MetaStore Server on port " + port);
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), hiveConf);
    isServerStarted = true;

    // This is default case with setugi off for both client and server
    createClient(false);
  }

  protected void createClient(boolean setugi) throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI,setugi);
    client = new HiveMetaStoreClient(hiveConf);
  }
}