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
package org.apache.hadoop.hive.metastore.datasource;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

public interface DataSourceProvider {

  /**
   * @param hdpConfig
   * @return the new connection pool
   */
  default DataSource create(Configuration hdpConfig) throws SQLException {
    int maxPoolSize = MetastoreConf.getIntVar(hdpConfig, MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS);
    return create(hdpConfig, maxPoolSize);
  }

  /**
   * @param hdpConfig
   * @param maxPoolSize the maximum size of the connection pool
   * @return the new connection pool
   */
  DataSource create(Configuration hdpConfig, int maxPoolSize) throws SQLException;

  /**
   * Get the declared pooling type string. This is used to check against the constant in
   * config options.
   * @return The pooling type string associated with the data source.
   */
  String getPoolingType();

  /**
   * @param hdpConfig
   * @return subset of properties prefixed by a connection pool specific substring
   */
  static Properties getPrefixedProperties(Configuration hdpConfig, String factoryPrefix) {
    Properties dataSourceProps = new Properties();
    Iterables.filter(
        hdpConfig, (entry -> entry.getKey() != null && entry.getKey().startsWith(factoryPrefix)))
        .forEach(entry -> dataSourceProps.put(entry.getKey(), entry.getValue()));
    return dataSourceProps;
  }

  static String getMetastoreJdbcUser(Configuration conf) {
    return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME);
  }

  static String getMetastoreJdbcPasswd(Configuration conf) throws SQLException {
    try {
      return MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.PWD);
    } catch (IOException err) {
      throw new SQLException("Error getting metastore password", err);
    }
  }

  static String getMetastoreJdbcDriverUrl(Configuration conf) throws SQLException {
    return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);
  }

  static String getDataSourceName(Configuration conf) {
    return conf.get(DataSourceNameConfigurator.DATA_SOURCE_NAME);
  }

  class DataSourceNameConfigurator implements Closeable {
    static final String DATA_SOURCE_NAME = "metastore.DataSourceProvider.pool.name";
    private final Configuration configuration;
    public DataSourceNameConfigurator(Configuration conf, String name) {
      this.configuration = conf;
      configuration.set(DATA_SOURCE_NAME, name);
    }
    public void resetName(String name) {
      configuration.set(DATA_SOURCE_NAME, name);
    }
    @Override
    public void close() {
      configuration.unset(DATA_SOURCE_NAME);
    }
  }
}
