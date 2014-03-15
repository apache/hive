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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.security;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProviderBase;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hcatalog.mapreduce.HCatStorageHandler;

/**
 * A HiveAuthorizationProvider which delegates the authorization requests to 
 * the underlying AuthorizationProviders obtained from the StorageHandler.
 * @deprecated 
 */
public class StorageDelegationAuthorizationProvider extends HiveAuthorizationProviderBase {

  protected HiveAuthorizationProvider hdfsAuthorizer = new HdfsAuthorizationProvider();

  protected static Map<String, String> authProviders = new HashMap<String, String>();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    hdfsAuthorizer.setConf(conf);
  }

  @Override
  public void init(Configuration conf) throws HiveException {
    hive_db = new HiveProxy(Hive.get(new HiveConf(conf, HiveAuthorizationProvider.class)));
  }

  @Override
  public void setAuthenticator(HiveAuthenticationProvider authenticator) {
    super.setAuthenticator(authenticator);
    hdfsAuthorizer.setAuthenticator(authenticator);
  }

  static {
    registerAuthProvider("org.apache.hadoop.hive.hbase.HBaseStorageHandler",
      "org.apache.hcatalog.hbase.HBaseAuthorizationProvider");
    registerAuthProvider("org.apache.hcatalog.hbase.HBaseHCatStorageHandler",
      "org.apache.hcatalog.hbase.HBaseAuthorizationProvider");
  }

  //workaround until Hive adds StorageHandler.getAuthorizationProvider(). Remove these parts afterwards
  public static void registerAuthProvider(String storageHandlerClass,
                      String authProviderClass) {
    authProviders.put(storageHandlerClass, authProviderClass);
  }

  /** Returns the StorageHandler of the Table obtained from the HCatStorageHandler */
  protected HiveAuthorizationProvider getDelegate(Table table) throws HiveException {
    HiveStorageHandler handler = table.getStorageHandler();

    if (handler != null) {
      if (handler instanceof HCatStorageHandler) {
        return ((HCatStorageHandler) handler).getAuthorizationProvider();
      } else {
        String authProviderClass = authProviders.get(handler.getClass().getCanonicalName());

        if (authProviderClass != null) {
          try {
            ReflectionUtils.newInstance(getConf().getClassByName(authProviderClass), getConf());
          } catch (ClassNotFoundException ex) {
            throw new HiveException("Cannot instantiate delegation AuthotizationProvider");
          }
        }

        //else we do not have anything to delegate to
        throw new HiveException(String.format("Storage Handler for table:%s is not an instance " +
          "of HCatStorageHandler", table.getTableName()));
      }
    } else {
      //return an authorizer for HDFS
      return hdfsAuthorizer;
    }
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    //global authorizations against warehouse hdfs directory
    hdfsAuthorizer.authorize(readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    //db's are tied to a hdfs location
    hdfsAuthorizer.authorize(db, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    getDelegate(table).authorize(table, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv,
              Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
    getDelegate(part.getTable()).authorize(part, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
              Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException,
    AuthorizationException {
    getDelegate(table).authorize(table, part, columns, readRequiredPriv, writeRequiredPriv);
  }
}
