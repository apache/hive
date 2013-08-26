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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.thrift.TException;

public abstract class HiveAuthorizationProviderBase implements
    HiveAuthorizationProvider {

  protected class HiveProxy {

    private final Hive hiveClient;
    private HMSHandler handler;

    public HiveProxy(Hive hive) {
      this.hiveClient = hive;
      this.handler = null;
    }

    public HiveProxy() {
      this.hiveClient = null;
      this.handler = null;
    }

    public void setHandler(HMSHandler handler){
      this.handler = handler;
    }

    public boolean isRunFromMetaStore(){
      return (this.hiveClient == null);
    }

    public PrincipalPrivilegeSet get_privilege_set(HiveObjectType column, String dbName,
        String tableName, List<String> partValues, String col, String userName,
        List<String> groupNames) throws HiveException {
      if (!isRunFromMetaStore()) {
        return hiveClient.get_privilege_set(
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

    public Database getDatabase(String dbName) throws HiveException {
      if (!isRunFromMetaStore()) {
        return hiveClient.getDatabase(dbName);
      } else {
        try {
          return handler.get_database(dbName);
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

  public static final Log LOG = LogFactory.getLog(
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

}
