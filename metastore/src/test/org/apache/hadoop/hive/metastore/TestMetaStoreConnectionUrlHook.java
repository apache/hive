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

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * TestMetaStoreConnectionUrlHook
 * Verifies that when an instance of an implementation of RawStore is initialized, the connection
 * URL has already been updated by any metastore connect URL hooks.
 */
public class TestMetaStoreConnectionUrlHook extends TestCase {
  private HiveConf hiveConf;

  @Override
  protected void setUp() throws Exception {

    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testUrlHook() throws Exception {
    hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLHOOK,
        DummyJdoConnectionUrlHook.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
        DummyJdoConnectionUrlHook.initialUrl);
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
        DummyRawStoreForJdoConnection.class.getName());
    hiveConf.setBoolean("hive.metastore.checkForDefaultDb", true);
    SessionState.start(new CliSessionState(hiveConf));

    // Instantiating the HMSHandler with hive.metastore.checkForDefaultDb will cause it to
    // initialize an instance of the DummyRawStoreForJdoConnection
    HiveMetaStore.HMSHandler hms = new HiveMetaStore.HMSHandler(
        "test_metastore_connection_url_hook_hms_handler", hiveConf);
  }
}
