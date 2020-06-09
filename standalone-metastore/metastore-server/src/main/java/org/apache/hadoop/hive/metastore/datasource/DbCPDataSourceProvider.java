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

import static org.apache.hadoop.hive.metastore.DatabaseProduct.MYSQL;
import static org.apache.hadoop.hive.metastore.DatabaseProduct.determineDatabaseProduct;

import java.sql.SQLException;

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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
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
  public DataSource create(Configuration hdpConfig) throws SQLException {
    LOG.debug("Creating dbcp connection pool for the MetaStore");

    String driverUrl = DataSourceProvider.getMetastoreJdbcDriverUrl(hdpConfig);
    String user = DataSourceProvider.getMetastoreJdbcUser(hdpConfig);
    String passwd = DataSourceProvider.getMetastoreJdbcPasswd(hdpConfig);

    BasicDataSource dbcpDs = new BasicDataSource();
    dbcpDs.setUrl(driverUrl);
    dbcpDs.setUsername(user);
    dbcpDs.setPassword(passwd);
    dbcpDs.setDefaultReadOnly(false);
    dbcpDs.setDefaultAutoCommit(true);

    DatabaseProduct dbProduct =  determineDatabaseProduct(driverUrl);
    switch (dbProduct){
      case MYSQL:
        dbcpDs.setConnectionProperties("allowMultiQueries=true");
        dbcpDs.setConnectionProperties("rewriteBatchedStatements=true");
        break;
      case POSTGRES:
        dbcpDs.setConnectionProperties("reWriteBatchedInserts=true");
        break;
    default:
      break;
    }
    int maxPoolSize = hdpConfig.getInt(
            MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS.getVarname(),
            ((Long) MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS.getDefaultVal()).intValue());
    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    int connectionMaxIlde = hdpConfig.getInt(CONNECTION_MAX_IDLE_PROPERTY, 8);
    int connectionMinIlde = hdpConfig.getInt(CONNECTION_MIN_IDLE_PROPERTY, 0);
    boolean testOnBorrow = hdpConfig.getBoolean(CONNECTION_TEST_BORROW_PROPERTY,
        BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW);
    long evictionTimeMillis = hdpConfig.getLong(CONNECTION_MIN_EVICT_MILLIS_PROPERTY,
        BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
    boolean testWhileIdle = hdpConfig.getBoolean(CONNECTION_TEST_IDLEPROPERTY,
        BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);
    long timeBetweenEvictionRuns = hdpConfig.getLong(CONNECTION_TIME_BETWEEN_EVICTION_RUNS_MILLIS,
        BaseObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
    int numTestsPerEvictionRun = hdpConfig.getInt(CONNECTION_NUM_TESTS_PER_EVICTION_RUN,
        BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
    boolean testOnReturn = hdpConfig.getBoolean(CONNECTION_TEST_ON_RETURN, BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
    long softMinEvictableIdleTimeMillis = hdpConfig.getLong(CONNECTION_SOFT_MIN_EVICTABLE_IDLE_TIME,
        BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
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
    objectPool.setMinEvictableIdleTimeMillis(evictionTimeMillis);
    objectPool.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRuns);
    objectPool.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    objectPool.setTestOnReturn(testOnReturn);
    objectPool.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    objectPool.setLifo(lifo);

    if (dbProduct == MYSQL) {
      poolableConnFactory.setValidationQuery("SET @@session.sql_mode=ANSI_QUOTES");
    }
    return new PoolingDataSource(objectPool);
  }

  @Override
  public String getPoolingType() {
    return DBCP;
  }
}
