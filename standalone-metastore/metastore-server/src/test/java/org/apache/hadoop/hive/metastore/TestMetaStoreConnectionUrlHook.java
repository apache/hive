/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * TestMetaStoreConnectionUrlHook
 * Verifies that when an instance of an implementation of RawStore is initialized, the connection
 * URL has already been updated by any metastore connect URL hooks.
 */
@Category(MetastoreUnitTest.class)
public class TestMetaStoreConnectionUrlHook {

  public static class DummyRawStoreForJdoConnection extends ObjectStore {
    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public void setConf(Configuration arg0) {
      String expected = DummyJdoConnectionUrlHook.newUrl;
      String actual = MetastoreConf.getVar(arg0, MetastoreConf.ConfVars.CONNECT_URL_KEY);

      Assert.assertEquals("The expected URL used by JDO to connect to the metastore: " + expected +
              " did not match the actual value when the Raw Store was initialized: " + actual,
          expected, actual);
    }

    @Override
    public void verifySchema() throws MetaException {
    }

    public void createCatalog(Catalog cat) throws MetaException {
    }

    @Override
    public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
      return null;
    }

    @Override
    public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    }

    @Override
    public Database getDatabase(String catName, String name) throws NoSuchObjectException {
      return null;
    }

    @Override
    public Role getRole(String roleName) throws NoSuchObjectException {
      return null;
    }

    @Override
    public boolean addRole(String roleName, String ownerName)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
      return false;
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privileges)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
      return false;
    }
  }

  @Test
  public void testUrlHook() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_HOOK, DummyJdoConnectionUrlHook.class.getName());
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, DummyJdoConnectionUrlHook.initialUrl);
    MetastoreConf.setVar(conf, ConfVars.RAW_STORE_IMPL, DummyRawStoreForJdoConnection.class.getName());
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    // Instantiating the HMSHandler with hive.metastore.checkForDefaultDb will cause it to
    // initialize an instance of the DummyRawStoreForJdoConnection
    HMSHandler hms = new HMSHandler("test_metastore_connection_url_hook_hms_handler", conf);
    hms.init();
  }
}
