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

import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * DataSourceProvider for the HikariCP connection pool.
 */
public class HikariCPDataSourceProvider implements DataSourceProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HikariCPDataSourceProvider.class);

  static final String HIKARI = "hikaricp";
  private static final String CONNECTION_TIMEOUT_PROPERTY = HIKARI + ".connectionTimeout";
  private static final String LEAK_DETECTION_THRESHOLD = HIKARI + ".leakDetectionThreshold";

  @Override
  public DataSource create(Configuration hdpConfig, int maxPoolSize) throws SQLException {
    String poolName = DataSourceProvider.getDataSourceName(hdpConfig);
    LOG.info("Creating Hikari connection pool for the MetaStore, maxPoolSize: {}, name: {}", maxPoolSize, poolName);

    String driverUrl = DataSourceProvider.getMetastoreJdbcDriverUrl(hdpConfig);
    String user = DataSourceProvider.getMetastoreJdbcUser(hdpConfig);
    String passwd = DataSourceProvider.getMetastoreJdbcPasswd(hdpConfig);

    Properties properties = replacePrefix(
        DataSourceProvider.getPrefixedProperties(hdpConfig, HIKARI));
    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    long leakDetectionThreshold = hdpConfig.getLong(LEAK_DETECTION_THRESHOLD, 3600000L);

    HikariConfig config;
    try {
      config = new HikariConfig(properties);
    } catch (Exception e) {
      throw new SQLException("Cannot create HikariCP configuration: ", e);
    }
    config.setMaximumPoolSize(maxPoolSize);
    config.setJdbcUrl(driverUrl);
    config.setUsername(user);
    config.setPassword(passwd);
    config.setLeakDetectionThreshold(leakDetectionThreshold);
    if (!StringUtils.isEmpty(poolName)) {
      config.setPoolName(poolName);
    }

    // It's kind of a waste to create a fixed size connection pool as same as the TxnHandler#connPool,
    // TxnHandler#connPoolMutex is mostly used for MutexAPI that is primarily designed to
    // provide coarse-grained mutex support to maintenance tasks running inside the Metastore,
    // add minimumIdle=2 and idleTimeout=10min(default, can be set by hikaricp.idleTimeout) to the pool,
    // so that the connection pool can retire the idle connection aggressively,
    // this will make Metastore more scalable especially if there is a leader in the warehouse.
    if ("mutex".equals(poolName)) {
      int minimumIdle = Integer.valueOf(hdpConfig.get(HIKARI + ".minimumIdle", "2"));
      config.setMinimumIdle(Math.min(maxPoolSize, minimumIdle));
    }

    //https://github.com/brettwooldridge/HikariCP
    config.setConnectionTimeout(connectionTimeout);

    DatabaseProduct dbProduct =  DatabaseProduct.determineDatabaseProduct(driverUrl, hdpConfig);

    String s = dbProduct.getPrepareTxnStmt();
    if (s!= null) {
      config.setConnectionInitSql(s);
    }

    Map<String, String> props = dbProduct.getDataSourceProperties();

    for ( Map.Entry<String, String> kv : props.entrySet()) {
      config.addDataSourceProperty(kv.getKey(), kv.getValue());
    }

    return new HikariDataSource(initMetrics(config));
  }

  @Override
  public String getPoolingType() {
    return HIKARI;
  }

  private Properties replacePrefix(Properties props) {
    Properties newProps = new Properties();
    props.forEach((key,value) ->
        newProps.put(key.toString().replaceFirst(HIKARI + ".", ""), value));
    return newProps;
  }

  private static HikariConfig initMetrics(final HikariConfig config) {
    final MetricRegistry registry = Metrics.getRegistry();
    if (registry != null) {
      config.setMetricRegistry(registry);
    }
    return config;
  }
}
