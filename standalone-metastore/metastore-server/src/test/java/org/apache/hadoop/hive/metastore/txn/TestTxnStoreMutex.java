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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.POOL_MUTEX;

/**
 * Tests for {@link TxnStoreMutex}, verifying that the mutex transaction is always completed
 * (committed or rolled back), even when the backend database fails while releasing the lock.
 * A transaction left open would stay bound to the thread and poison every subsequent
 * {@link TxnStoreMutex#acquireLock(String)} call made from the same thread.
 */
@Category(MetastoreUnitTest.class)
public class TestTxnStoreMutex {

  private Configuration conf;
  private FaultInjectingDataSource dataSource;
  private TxnStoreMutex mutex;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);

    dataSource = new FaultInjectingDataSource(new DriverManagerDataSource(
        MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY),
        MetastoreConf.getVar(conf, ConfVars.CONNECTION_USER_NAME),
        MetastoreConf.getPassword(conf, ConfVars.PWD)));

    DatabaseProduct dbProduct = DatabaseProduct.determineDatabaseProduct(dataSource, conf);
    SQLGenerator sqlGenerator = new SQLGenerator(dbProduct, conf);
    MultiDataSourceJdbcResource jdbcResource = new MultiDataSourceJdbcResource(dbProduct, conf, sqlGenerator);
    jdbcResource.registerDataSource(POOL_MUTEX, dataSource);
    mutex = new TxnStoreMutex(sqlGenerator, jdbcResource);
  }

  @After
  public void tearDown() throws Exception {
    // Defensive cleanup so a leaked transaction cannot poison other tests running on this thread:
    // roll back and close the leaked connection, otherwise it keeps holding its Derby row locks.
    if (dataSource != null && TransactionSynchronizationManager.hasResource(dataSource)) {
      ConnectionHolder holder = (ConnectionHolder) TransactionSynchronizationManager.unbindResource(dataSource);
      try (Connection connection = holder.getConnection()) {
        connection.rollback();
      }
    }
    // A leaked transaction also leaves the synchronization thread-locals initialized, which would
    // make every later transaction on this thread skip its own synchronization cleanup on commit.
    TransactionSynchronizationManager.clear();
    TestTxnDbUtil.cleanDb(conf);
  }

  @Test
  public void testReleaseLocksCompletesTransactionOnUpdateFailure() throws Exception {
    TxnStore.MutexAPI.LockHandle handle = mutex.acquireLock(TxnStore.MUTEX_KEY.Initiator.name());

    dataSource.failAuxTableUpdate = true;
    try {
      handle.releaseLocks(System.currentTimeMillis());
    } finally {
      dataSource.failAuxTableUpdate = false;
    }

    Assert.assertFalse("Mutex transaction was left bound to the thread after a failed release",
        TransactionSynchronizationManager.hasResource(dataSource));
    Assert.assertFalse("A transaction is still active after a failed release",
        TransactionSynchronizationManager.isActualTransactionActive());

    // The same thread must be able to run a full acquire/release cycle afterwards.
    TxnStore.MutexAPI.LockHandle recovered = mutex.acquireLock(TxnStore.MUTEX_KEY.Initiator.name());
    recovered.releaseLocks(System.currentTimeMillis());
    Assert.assertFalse(TransactionSynchronizationManager.hasResource(dataSource));
  }

  /**
   * Delegates to a real {@link DataSource}, but while {@link #failAuxTableUpdate} is set, connections
   * throw a {@link SQLException} (SQLState 08S01, like a lost connection) on any statement preparation.
   * The flag is armed only around {@code releaseLocks(Long)}, where the AUX_TABLE update is the sole
   * statement prepared on the transaction's connection.
   */
  private static final class FaultInjectingDataSource extends DelegatingDataSource {

    boolean failAuxTableUpdate;

    FaultInjectingDataSource(DataSource delegate) {
      super(delegate);
    }

    @Override
    public Connection getConnection() throws SQLException {
      Connection real = super.getConnection();
      return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Connection.class },
          (proxy, method, args) -> {
            if (failAuxTableUpdate && "prepareStatement".equals(method.getName())) {
              throw new SQLException("Simulated lost connection to the backend DB", "08S01");
            }
            try {
              return method.invoke(real, args);
            } catch (InvocationTargetException e) {
              throw e.getTargetException();
            }
          });
    }
  }
}
