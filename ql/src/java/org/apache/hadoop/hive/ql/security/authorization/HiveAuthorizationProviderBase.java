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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.List;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePolicyProvider;
import org.apache.thrift.TException;

public abstract class HiveAuthorizationProviderBase implements
    HiveAuthorizationProvider {

  protected class HiveProxy {

    private final boolean hasHiveClient;
    private final HiveConf conf;
    private IHMSHandler handler;

    public HiveProxy(Hive hive) {
      this.hasHiveClient = hive != null;
      this.conf = hive.getConf();
      this.handler = null;
    }

    public HiveProxy() {
      this.hasHiveClient = false;
      this.conf = null;
      this.handler = null;
    }

    public void setHandler(IHMSHandler handler){
      this.handler = handler;
    }

    public boolean isRunFromMetaStore(){
      return !hasHiveClient;
    }

    public PrincipalPrivilegeSet get_privilege_set(HiveObjectType column, String dbName,
        String tableName, List<String> partValues, String col, String userName,
        List<String> groupNames) throws HiveException {
      if (!isRunFromMetaStore()) {
        return Hive.getWithFastCheck(conf).get_privilege_set(
            column, dbName, tableName, partValues, col, userName, groupNames);
      } else {
        HiveObjectRef hiveObj = new HiveObjectRef(column, dbName,
            tableName, partValues, col);
        try {
          return handler.get_privilege_set(hiveObj, userName, groupNames);
        } catch (MetaException e) {
          throw new HiveException(e);
        } catch (TException e) {
          throw new HiveException(e);
        }
      }
    }

    /**
     * Get the database object
     * @param catName catalog name.  If null, the default will be pulled from the conf.  This
     *                means the caller does not have to check isCatNameSet()
     * @param dbName database name.
     * @return
     * @throws HiveException
     */
    public Database getDatabase(String catName, String dbName) throws HiveException {
      catName = catName == null ? MetaStoreUtils.getDefaultCatalog(conf) : catName;
      if (!isRunFromMetaStore()) {
        return Hive.getWithFastCheck(conf).getDatabase(catName, dbName);
      } else {
        try {
          return handler.get_database_core(catName, dbName);
        } catch (NoSuchObjectException e) {
          throw new HiveException(e);
        } catch (MetaException e) {
          throw new HiveException(e);
        }
      }
    }

  }

  protected HiveProxy hive_db;

  protected HiveAuthenticationProvider authenticator;

  private Configuration conf;

  public static final Logger LOG = LoggerFactory.getLogger(
      HiveAuthorizationProvider.class);


  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      init(conf);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public HiveAuthenticationProvider getAuthenticator() {
    return authenticator;
  }

  public void setAuthenticator(HiveAuthenticationProvider authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public HivePolicyProvider getHivePolicyProvider() throws HiveAuthzPluginException {
    return null;
  }
}
