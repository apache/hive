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

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import com.jolbox.bonecp.StatisticsMBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataSourceProvider for the BoneCP connection pool.
 */
public class BoneCPDataSourceProvider implements DataSourceProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BoneCPDataSourceProvider.class);

  public static final String BONECP = "bonecp";
  private static final String CONNECTION_TIMEOUT_PROPERTY= BONECP + ".connectionTimeoutInMs";
  private static final String PARTITION_COUNT_PROPERTY= BONECP + ".partitionCount";

  @Override
  public DataSource create(Configuration hdpConfig) throws SQLException {

    LOG.debug("Creating BoneCP connection pool for the MetaStore");

    String driverUrl = DataSourceProvider.getMetastoreJdbcDriverUrl(hdpConfig);
    String user = DataSourceProvider.getMetastoreJdbcUser(hdpConfig);
    String passwd = DataSourceProvider.getMetastoreJdbcPasswd(hdpConfig);
    int maxPoolSize = MetastoreConf.getIntVar(hdpConfig,
        MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS);

    Properties properties = DataSourceProvider.getPrefixedProperties(hdpConfig, BONECP);
    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    String partitionCount = properties.getProperty(PARTITION_COUNT_PROPERTY, "1");

    BoneCPConfig config = null;
    try {
      config = new BoneCPConfig(properties);
    } catch (Exception e) {
      throw new SQLException("Cannot create BoneCP configuration: ", e);
    }
    config.setJdbcUrl(driverUrl);
    // if we are waiting for connection for a long time, something is really wrong
    // better raise an error than hang forever
    // see DefaultConnectionStrategy.getConnectionInternal()
    config.setConnectionTimeoutInMs(connectionTimeout);
    config.setMaxConnectionsPerPartition(maxPoolSize);
    config.setPartitionCount(Integer.parseInt(partitionCount));
    config.setUser(user);
    config.setPassword(passwd);

    return initMetrics(new BoneCPDataSource(config));
  }

  @Override
  public boolean mayReturnClosedConnection() {
    // See HIVE-11915 for details
    return true;
  }

  @Override
  public String getPoolingType() {
    return BONECP;
  }

  private BoneCPDataSource initMetrics(BoneCPDataSource ds) {
    final MetricRegistry registry = Metrics.getRegistry();
    if (registry != null) {
      registry.registerAll(new BoneCPMetrics(ds));
    }
    return ds;
  }

  private static class BoneCPMetrics implements MetricSet {
    private BoneCPDataSource ds;
    private Optional<String> poolName;

    private BoneCPMetrics(final BoneCPDataSource ds) {
      this.ds = ds;
      this.poolName = Optional.ofNullable(ds.getPoolName());
    }

    private String name(final String gaugeName) {
      return poolName.orElse("BoneCP") + ".pool." + gaugeName;
    }

    @Override
    public Map<String, Metric> getMetrics() {
      final Map<String, Metric> gauges = new HashMap<>();

      gauges.put(name("TotalConnections"), new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          if (ds.getPool() != null) {
            return ds.getPool().getStatistics().getTotalCreatedConnections();
          } else {
            return 0;
          }
        }
      });

      gauges.put(name("IdleConnections"), new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          if (ds.getPool() != null) {
            return ds.getPool().getStatistics().getTotalFree();
          } else {
            return 0;
          }
        }
      });

      gauges.put(name("ActiveConnections"), new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          if (ds.getPool() != null) {
            return ds.getPool().getStatistics().getTotalLeased();
          } else {
            return 0;
          }
        }
      });

      gauges.put(name("WaitTimeAvg"), new Gauge<Double>() {
        @Override
        public Double getValue() {
          if (ds.getPool() != null) {
            return ds.getPool().getStatistics().getConnectionWaitTimeAvg();
          } else {
            return 0.0;
          }
        }
      });

      return Collections.unmodifiableMap(gauges);
    }
  }

}
