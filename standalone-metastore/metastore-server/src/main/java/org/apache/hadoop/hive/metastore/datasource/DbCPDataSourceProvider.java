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
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;

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

  @Override
  public DataSource create(Configuration hdpConfig) throws SQLException {

    LOG.debug("Creating dbcp connection pool for the MetaStore");

    String driverUrl = DataSourceProvider.getMetastoreJdbcDriverUrl(hdpConfig);
    String user = DataSourceProvider.getMetastoreJdbcUser(hdpConfig);
    String passwd = DataSourceProvider.getMetastoreJdbcPasswd(hdpConfig);
    int maxPoolSize = hdpConfig.getInt(
            MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS.getVarname(),
            ((Long) MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS.getDefaultVal()).intValue());
    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    int connectionMaxIlde = hdpConfig.getInt(CONNECTION_MAX_IDLE_PROPERTY, GenericObjectPool.DEFAULT_MAX_IDLE);
    int connectionMinIlde = hdpConfig.getInt(CONNECTION_MIN_IDLE_PROPERTY, GenericObjectPool.DEFAULT_MIN_IDLE);
    boolean testOnBorrow = hdpConfig.getBoolean(CONNECTION_TEST_BORROW_PROPERTY,
            GenericObjectPool.DEFAULT_TEST_ON_BORROW);
    long evictionTimeMillis = hdpConfig.getLong(CONNECTION_MIN_EVICT_MILLIS_PROPERTY,
            GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
    boolean testWhileIdle = hdpConfig.getBoolean(CONNECTION_TEST_IDLEPROPERTY,
            GenericObjectPool.DEFAULT_TEST_WHILE_IDLE);
    long timeBetweenEvictionRuns = hdpConfig.getLong(CONNECTION_TIME_BETWEEN_EVICTION_RUNS_MILLIS,
            GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
    int numTestsPerEvictionRun = hdpConfig.getInt(CONNECTION_NUM_TESTS_PER_EVICTION_RUN,
            GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
    boolean testOnReturn = hdpConfig.getBoolean(CONNECTION_TEST_ON_RETURN, GenericObjectPool.DEFAULT_TEST_ON_RETURN);
    long softMinEvictableIdleTimeMillis = hdpConfig.getLong(CONNECTION_SOFT_MIN_EVICTABLE_IDLE_TIME,
            GenericObjectPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
    boolean lifo = hdpConfig.getBoolean(CONNECTION_LIFO, GenericObjectPool.DEFAULT_LIFO);

    GenericObjectPool objectPool = new GenericObjectPool();
    objectPool.setMaxActive(maxPoolSize);
    objectPool.setMaxWait(connectionTimeout);
    objectPool.setMaxIdle(connectionMaxIlde);
    objectPool.setMinIdle(connectionMinIlde);
    objectPool.setTestOnBorrow(testOnBorrow);
    objectPool.setTestWhileIdle(testWhileIdle);
    objectPool.setMinEvictableIdleTimeMillis(evictionTimeMillis);
    objectPool.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRuns);
    objectPool.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    objectPool.setTestOnReturn(testOnReturn);
    objectPool.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    objectPool.setLifo(lifo);

    ConnectionFactory connFactory = new DriverManagerConnectionFactory(driverUrl, user, passwd);
    // This doesn't get used, but it's still necessary, see
    // https://git1-us-west.apache.org/repos/asf?p=commons-dbcp.git;a=blob;f=doc/ManualPoolingDataSourceExample.java;
    // h=f45af2b8481f030b27364e505984c0eef4f35cdb;hb=refs/heads/DBCP_1_5_x_BRANCH
    new PoolableConnectionFactory(connFactory, objectPool, null, null, false, true);

    return new PoolingDataSource(objectPool);
  }

  @Override
  public boolean mayReturnClosedConnection() {
    // Only BoneCP should return true
    return false;
  }

  @Override
  public String getPoolingType() {
    return DBCP;
  }
}
