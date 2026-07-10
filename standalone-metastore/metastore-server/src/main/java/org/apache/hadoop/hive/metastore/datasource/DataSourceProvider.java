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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import javax.sql.DataSource;

import com.google.common.collect.Iterables;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

public interface DataSourceProvider {
  String METASTORE_CONF_PROPERTY = "metastore.jdbc.configuration";
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

  static String getMetastoreJdbcDriverUrl(Configuration conf) {
    if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_PROFILE_JDBC_EXECUTION)) {
      return MetastoreDriver.getMetastoreDbUrl(conf);
    }
    return MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);
  }

  static void addJdbcWrapperProperties(Configuration configuration, Properties properties)
      throws SQLException {
    if (MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.METASTORE_PROFILE_JDBC_EXECUTION)) {
      try {
        Configuration slimConf = new Configuration(false);
        configuration.getPropsWithPrefix("metastore.jdbc.")
            .forEach((k,v) -> slimConf.set("metastore.jdbc." + k, v));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        slimConf.writeXml(outputStream);
        properties.put(METASTORE_CONF_PROPERTY, Base64.getEncoder().encodeToString(outputStream.toByteArray()));
      } catch (IOException e) {
        throw new SQLException("Failed to serialize Metastore configuration", e);
      }
    }
  }

  static Configuration resolveConfiguration(Properties properties) {
    if (properties != null) {
      String encodedConfiguration = properties.getProperty(METASTORE_CONF_PROPERTY);
      if (StringUtils.isNotEmpty(encodedConfiguration)) {
        byte[] xmlBytes = Base64.getDecoder().decode(encodedConfiguration);
        Configuration configuration = new Configuration(false);
        configuration.addResource(new ByteArrayInputStream(xmlBytes), "metastore-jdbc-configuration");
        return configuration;
      }
    }
    throw new IllegalStateException("properties should have contained the information for Metastore profiling");
  }

  static String getDataSourceName(Configuration conf) {
    return conf.get(DataSourceNameConfigurator.DATA_SOURCE_NAME);
  }

  static void preparePool(Configuration configuration, Consumer<String> initSql,
      Consumer<Map.Entry<String, String>> dataSourceProps) {
    String url = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CONNECT_URL_KEY);
    DatabaseProduct dbProduct =  DatabaseProduct.determineDatabaseProduct(url, configuration);
    String s = dbProduct.getPrepareTxnStmt();
    if (s != null) {
      initSql.accept(s);
    }
    Map<String, String> properties = dbProduct.getDataSourceProperties();
    properties.entrySet().forEach(dataSourceProps);
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
