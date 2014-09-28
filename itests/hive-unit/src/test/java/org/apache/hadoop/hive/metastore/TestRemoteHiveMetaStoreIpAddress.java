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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * TestRemoteHiveMetaStoreIpAddress.
 *
 * Test which checks that the remote Hive metastore stores the proper IP address using
 * IpAddressListener
 */
public class TestRemoteHiveMetaStoreIpAddress extends TestCase {
  private static boolean isServerStarted = false;
  private static HiveConf hiveConf;
  private static HiveMetaStoreClient msc;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());

    if (isServerStarted) {
      assertNotNull("Unable to connect to the MetaStore server", msc);
      return;
    }

    int port = MetaStoreUtils.findFreePort();
    System.out.println("Starting MetaStore Server on port " + port);
    System.setProperty(ConfVars.METASTORE_EVENT_LISTENERS.varname,
        IpAddressListener.class.getName());
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());
    isServerStarted = true;

    // This is default case with setugi off for both client and server
    createClient(port);
  }

  public void testIpAddress() throws Exception {
    try {

      Database db = new Database();
      db.setName("testIpAddressIp");
      msc.createDatabase(db);
      msc.dropDatabase(db.getName());
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testIpAddress() failed.");
      throw e;
    }
  }

  protected void createClient(int port) throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    msc = new HiveMetaStoreClient(hiveConf);
  }
}
