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
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataSourceProvider for the dbcp connection pool.
 */
public class DbCPDataSourceProvider implements DataSourceProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DbCPDataSourceProvider.class);

  static final String DBCP = "dbcp";
  private static final String CONNECTION_TIMEOUT_PROPERTY = DBCP + ".maxWait";
  private static final String CONNECTION_MAX_IDLE_PROPERTY = DBCP + ".maxIdle";
  private static final String CONNECTION_MIN_IDLE_PROPERTY = DBCP + ".minIdle";
  private static final String CONNECTION_TEST_BORROW_PROPERTY = DBCP + ".testOnBorrow";
  private static final String CONNECTION_MIN_EVICT_MILLIS_PROPERTY = DBCP + ".minEvictableIdleTimeMillis";
  private static final String CONNECTION_TEST_IDLEPROPERTY = DBCP + ".testWhileIdle";
  private static final String CONNECTION_TIME_BETWEEN_EVICTION_RUNS_MILLIS = DBCP + ".timeBetweenEvictionRunsMillis";
  private static final String CONNECTION_NUM_TESTS_PER_EVICTION_RUN = DBCP + ".numTestsPerEvictionRun";
  private static final String CONNECTION_TEST_ON_RETURN = DBCP + ".testOnReturn";
  private static final String CONNECTION_SOFT_MIN_EVICTABLE_IDLE_TIME = DBCP + ".softMinEvictableIdleTimeMillis";
  private static final String CONNECTION_LIFO = DBCP + ".lifo";

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public DataSource create(Configuration hdpConfig, int maxPoolSize) throws SQLException {
    String poolName = DataSourceProvider.getDataSourceName(hdpConfig);
    LOG.info("Creating dbcp connection pool for the MetaStore, maxPoolSize: {}, name: {}", maxPoolSize, poolName);

    String driverUrl = DataSourceProvider.getMetastoreJdbcDriverUrl(hdpConfig);
    String user = DataSourceProvider.getMetastoreJdbcUser(hdpConfig);
    String passwd = DataSourceProvider.getMetastoreJdbcPasswd(hdpConfig);

    BasicDataSource dbcpDs = new BasicDataSource();
    dbcpDs.setUrl(driverUrl);
    dbcpDs.setUsername(user);
    dbcpDs.setPassword(passwd);
    dbcpDs.setDefaultReadOnly(false);
    dbcpDs.setDefaultAutoCommit(true);

    DatabaseProduct dbProduct =  DatabaseProduct.determineDatabaseProduct(driverUrl, hdpConfig);
    Map<String, String> props = dbProduct.getDataSourceProperties();
    for (Map.Entry<String, String> kv : props.entrySet()) {
      dbcpDs.setConnectionProperties(kv.getKey() + "=" + kv.getValue());
    }

    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    int connectionMaxIlde = hdpConfig.getInt(CONNECTION_MAX_IDLE_PROPERTY, 8);
    int connectionMinIlde = hdpConfig.getInt(CONNECTION_MIN_IDLE_PROPERTY, 0);
    boolean testOnBorrow = hdpConfig.getBoolean(CONNECTION_TEST_BORROW_PROPERTY,
        BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW);
    long evictionTimeMillis = hdpConfig.getLong(CONNECTION_MIN_EVICT_MILLIS_PROPERTY,
        BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME.toMillis());
    boolean testWhileIdle = hdpConfig.getBoolean(CONNECTION_TEST_IDLEPROPERTY,
        BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);
    long timeBetweenEvictionRuns = hdpConfig.getLong(CONNECTION_TIME_BETWEEN_EVICTION_RUNS_MILLIS,
        BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS.toMillis());
    int numTestsPerEvictionRun = hdpConfig.getInt(CONNECTION_NUM_TESTS_PER_EVICTION_RUN,
        BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
    boolean testOnReturn = hdpConfig.getBoolean(CONNECTION_TEST_ON_RETURN, BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
    long softMinEvictableIdleTimeMillis = hdpConfig.getLong(CONNECTION_SOFT_MIN_EVICTABLE_IDLE_TIME,
        BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME.toMillis());
    boolean lifo = hdpConfig.getBoolean(CONNECTION_LIFO, BaseObjectPoolConfig.DEFAULT_LIFO);

    ConnectionFactory connFactory = new DataSourceConnectionFactory(dbcpDs);
    PoolableConnectionFactory poolableConnFactory = new PoolableConnectionFactory(connFactory, null);

    GenericObjectPool objectPool = new GenericObjectPool(poolableConnFactory);
    objectPool.setMaxTotal(maxPoolSize);
    objectPool.setMaxWaitMillis(connectionTimeout);
    objectPool.setMaxIdle(connectionMaxIlde);
    objectPool.setMinIdle(connectionMinIlde);
    objectPool.setTestOnBorrow(testOnBorrow);
    objectPool.setTestWhileIdle(testWhileIdle);
    objectPool.setMinEvictableIdleTime(Duration.ofMillis(evictionTimeMillis));
    objectPool.setTimeBetweenEvictionRuns(Duration.ofMillis(timeBetweenEvictionRuns));
    objectPool.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    objectPool.setTestOnReturn(testOnReturn);
    objectPool.setSoftMinEvictableIdleTime(Duration.ofMillis(softMinEvictableIdleTimeMillis));
    objectPool.setLifo(lifo);

    // Enable TxnHandler#connPoolMutex to release the idle connection if possible,
    // TxnHandler#connPoolMutex is mostly used for MutexAPI that is primarily designed to
    // provide coarse-grained mutex support to maintenance tasks running inside the Metastore,
    // this will make Metastore more scalable especially if there is a leader in the warehouse.
    if ("mutex".equalsIgnoreCase(poolName)) {
      if (timeBetweenEvictionRuns < 0) {
        // When timeBetweenEvictionRunsMillis non-positive, no idle object evictor thread runs
        objectPool.setTimeBetweenEvictionRuns(Duration.ofMillis(30 * 1000));
      }
      if (softMinEvictableIdleTimeMillis < 0) {
        objectPool.setSoftMinEvictableIdleTime(Duration.ofMillis(600 * 1000));
      }
    }
    String stmt = dbProduct.getPrepareTxnStmt();
    if (stmt != null) {
      poolableConnFactory.setConnectionInitSql(Collections.singletonList(stmt));
    }
    return new PoolingDataSource(objectPool);
  }

  @Override
  public String getPoolingType() {
    return DBCP;
  }
}
