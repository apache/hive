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

package org.apache.hadoop.hive.metastore.dataconnector;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.DERBY_TYPE;
import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.HIVE_JDBC_TYPE;
import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.MSSQL_TYPE;
import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.MYSQL_TYPE;
import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.ORACLE_TYPE;
import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.POSTGRES_TYPE;

public class DataConnectorProviderFactory {
  static final Logger LOG = LoggerFactory.getLogger(DataConnectorProviderFactory.class);

  private static Cache<String, IDataConnectorProvider> dataConnectorCache = null;
  private static DataConnectorProviderFactory singleton = null;
  private static IHMSHandler hmsHandler = null;

  private static class CacheRemoveListener implements RemovalListener<String, IDataConnectorProvider> {
    @Override
    public void onRemoval(@Nullable String dcName, @Nullable IDataConnectorProvider dataConnectorProvider,
                          @NonNull RemovalCause cause) {
      try {
        LOG.info("Closing dataConnectorProvider :{}", dcName);
        dataConnectorProvider.close();
      } catch (Exception e) {
        LOG.warn("Exception when closing dataConnectorProvider: {} due to: {}" + dcName, e.getMessage());
      }
    }
  }

  private DataConnectorProviderFactory(IHMSHandler hmsHandler) {
    dataConnectorCache = Caffeine.newBuilder()
        .removalListener(new CacheRemoveListener())
        .maximumSize(100)
        .expireAfterAccess(1, TimeUnit.HOURS).build();
    this.hmsHandler = hmsHandler;
  }

  public static synchronized DataConnectorProviderFactory getInstance(IHMSHandler hmsHandler) {
    if (singleton == null) {
      singleton = new DataConnectorProviderFactory(hmsHandler);
    }
    return singleton;
  }

  public static synchronized IDataConnectorProvider getDataConnectorProvider(Database db) throws MetaException {
    IDataConnectorProvider provider = null;
    DataConnector connector = null;
    if (db.getType() == DatabaseType.NATIVE) {
      throw new MetaException("Database " + db.getName() + " is of type NATIVE, no connector available");
    }

    String scopedDb = (db.getRemote_dbname() != null) ? db.getRemote_dbname() : db.getName();
    provider = dataConnectorCache.getIfPresent(db.getConnector_name().toLowerCase());
    if (provider != null) {
      provider.setScope(scopedDb);
      return provider;
    }

    try {
      connector = hmsHandler.get_dataconnector_core(db.getConnector_name());
    } catch (NoSuchObjectException notexists) {
      throw new MetaException("Data connector " + db.getConnector_name() + " associated with database "
          + db.getName() + " does not exist");
    }
    String type = connector.getType();
    switch (type) {
    case DERBY_TYPE:
    case HIVE_JDBC_TYPE:
    case MSSQL_TYPE:
    case MYSQL_TYPE:
    case ORACLE_TYPE:
    case POSTGRES_TYPE:
      try {
        provider = JDBCConnectorProviderFactory.get(scopedDb, connector);
      } catch (Exception e) {
        throw new MetaException("Could not instantiate a provider for database " + db.getName());
      }
      break;
    default:
      throw new MetaException("Data connector of type " + connector.getType() + " not implemented yet");
    }
    dataConnectorCache.put(connector.getName().toLowerCase(), provider);
    return provider;
  }

  /**
   * After executing Drop or Alter DDL on a dataConnector, we should update cache to clean the dataConnector
   * to avoid using the invalid dataConnector next time.
   * @param dcName dataConnector to be cleaned
   */
  public static synchronized void invalidateDataConnectorFromCache(String dcName) {
    try {
      IDataConnectorProvider dataConnectorProvider = dataConnectorCache.getIfPresent(dcName);
      if (dataConnectorProvider != null) {
        dataConnectorCache.invalidate(dcName);
      }
    } catch (Exception e) {
      LOG.warn("Exception when removing dataConnectorProvider: {} from cache due to: {}" + dcName, e.getMessage());
    }
  }
}
