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

package org.apache.hadoop.hive.metastore.metastore;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;

import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import java.util.List;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.ExceptionHandler;
import org.apache.hadoop.hive.metastore.directsql.MetaStoreDirectSql;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.datanucleus.api.jdo.JDOTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for getting stuff w/transaction, direct SQL, perf logging, etc. */
@VisibleForTesting
public abstract class GetHelper<T> {
  private static final Logger LOG = LoggerFactory.getLogger(GetHelper.class);
  private static final Counter directSqlErrors = Metrics.getRegistry() != null ?
      Metrics.getOrCreateCounter(MetricsConstants.DIRECTSQL_ERRORS) : new Counter();
  private final boolean isInTxn, doTrace, allowJdo;
  private boolean doUseDirectSql;
  private long start;
  private Table table;
  protected final RawStore baseStore;
  protected final PersistenceManager pm;
  private MetaStoreDirectSql directSql;
  protected final List<String> partitionFields;
  protected final TableName tableName;
  private boolean success = false;
  protected T results = null;

  public GetHelper(RawStoreAware rsa, TableName tableName) throws MetaException {
    this(rsa, tableName, null);
  }

  public GetHelper(RawStoreAware rsa,
      TableName tableName, List<String> fields) throws MetaException {
    this.baseStore = rsa.getBaseStore();
    this.partitionFields = fields;
    this.tableName = tableName;
    this.doTrace = LOG.isDebugEnabled();
    this.isInTxn = baseStore.isActiveTransaction();
    this.pm = rsa.getPersistentManager();
    this.allowJdo = canUseJdoQuery(this);

    boolean isConfigEnabled = MetastoreConf.getBoolVar(baseStore.getConf(),
        MetastoreConf.ConfVars.TRY_DIRECT_SQL);
    if (isConfigEnabled) {
      directSql = new MetaStoreDirectSql(pm, baseStore.getConf(), "");
    }

    if (!allowJdo && isConfigEnabled && !directSql.isCompatibleDatastore()) {
      throw new MetaException("SQL is not operational"); // test path; SQL is enabled and broken.
    }
    this.doUseDirectSql = isConfigEnabled && directSql.isCompatibleDatastore();
  }

  protected boolean canUseDirectSql(GetHelper<T> ctx) throws MetaException {
    return true; // By default, assume we can user directSQL - that's kind of the point.
  }

  protected boolean canUseJdoQuery(GetHelper<T> ctx) throws MetaException {
    return true;
  }

  protected abstract String describeResult();
  protected abstract T getSqlResult(GetHelper<T> ctx) throws MetaException;
  protected abstract T getJdoResult(GetHelper<T> ctx)
      throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException;

  public T run(boolean initTable) throws MetaException, NoSuchObjectException {
    try {
      start(initTable);
      String savePoint = isInTxn && allowJdo ? "rollback_" + System.nanoTime() : null;
      if (doUseDirectSql) {
        try {
          directSql.prepareTxn();
          setTransactionSavePoint(savePoint);
          this.results = getSqlResult(this);
          LOG.debug("Using direct SQL optimization.");
        } catch (Exception ex) {
          handleDirectSqlError(ex, savePoint);
        }
      }
      // Note that this will be invoked in 2 cases:
      //    1) DirectSQL was disabled to start with;
      //    2) DirectSQL threw and was disabled in handleDirectSqlError.
      if (!doUseDirectSql && canUseJdoQuery(this)) {
        this.results = getJdoResult(this);
        LOG.debug("Not using direct SQL optimization.");
      }
      return commit();
    } catch (NoSuchObjectException | MetaException ex) {
      throw ex;
    } catch (Exception ex) {
      LOG.error("", ex);
      throw new MetaException(ex.getMessage());
    } finally {
      close();
    }
  }

  private void start(boolean initTable) throws MetaException, NoSuchObjectException {
    start = doTrace ? System.nanoTime() : 0;
    baseStore.openTransaction();
    if (initTable && (tableName != null)) {
      TableStore store = baseStore.unwrap(TableStore.class);
      table = store.getTable(tableName, null, -1);
      if (table == null) {
        throw new NoSuchObjectException(
            "Specified catalog.database.table does not exist : " + tableName);
      }
    }
    doUseDirectSql = doUseDirectSql && canUseDirectSql(this);
  }

  private void handleDirectSqlError(Exception ex, String savePoint) throws MetaException, NoSuchObjectException {
    String message = null;
    try {
      message = generateShorterMessage(ex);
    } catch (Throwable t) {
      message = ex.toString() + "; error building a better message: " + t.getMessage();
    }
    LOG.warn(message); // Don't log the exception, people just get confused.
    LOG.debug("Full DirectSQL callstack for debugging (not an error)", ex);

    if (!allowJdo || !DatabaseProduct.isRecoverableException(ex)) {
      throw ExceptionHandler.newMetaException(ex);
    }

    if (!isInTxn) {
      JDOException rollbackEx = null;
      try {
        baseStore.rollbackTransaction();
      } catch (JDOException jex) {
        rollbackEx = jex;
      }
      if (rollbackEx != null) {
        // Datanucleus propagates some pointless exceptions and rolls back in the finally.
        if (baseStore.isActiveTransaction()) {
          throw rollbackEx; // Throw if the tx wasn't rolled back.
        }
        LOG.info("Ignoring exception, rollback succeeded: " + rollbackEx.getMessage());
      }

      start = doTrace ? System.nanoTime() : 0;
      baseStore.openTransaction();
      if (table != null) {
        TableStore store = baseStore.unwrap(TableStore.class);
        table = store.getTable(tableName, null, -1);
        if (table == null) {
          throw new NoSuchObjectException(
              "Specified catalog.database.table does not exist : " + tableName);
        }
      }
    } else {
      rollbackTransactionToSavePoint(savePoint);
      start = doTrace ? System.nanoTime() : 0;
    }

    directSqlErrors.inc();
    doUseDirectSql = false;
  }

  private void setTransactionSavePoint(String savePoint) {
    if (savePoint != null) {
      ((JDOTransaction) pm.currentTransaction()).setSavepoint(savePoint);
    }
  }

  private void rollbackTransactionToSavePoint(String savePoint) {
    if (savePoint != null) {
      ((JDOTransaction) pm.currentTransaction()).rollbackToSavepoint(savePoint);
    }
  }

  private String generateShorterMessage(Exception ex) {
    StringBuilder message = new StringBuilder(
        "Falling back to ORM path due to direct SQL failure (this is not an error): ");
    Throwable t = ex;
    StackTraceElement[] prevStack = null;
    while (t != null) {
      message.append(t.getMessage());
      StackTraceElement[] stack = t.getStackTrace();
      int uniqueFrames = stack.length - 1;
      if (prevStack != null) {
        int n = prevStack.length - 1;
        while (uniqueFrames >= 0 && n >= 0 && stack[uniqueFrames].equals(prevStack[n])) {
          uniqueFrames--; n--;
        }
      }
      for (int i = 0; i <= uniqueFrames; ++i) {
        StackTraceElement ste = stack[i];
        message.append(" at ").append(ste);
        if (ste.getMethodName().contains("getSqlResult")
            && (ste.getFileName() == null || ste.getFileName().contains("ObjectStore"))) {
          break;
        }
      }
      prevStack = stack;
      t = t.getCause();
      if (t != null) {
        message.append(";\n Caused by: ");
      }
    }
    return message.toString();
  }

  private T commit() {
    success = baseStore.commitTransaction();
    if (doTrace) {
      double time = ((System.nanoTime() - start) / 1000000.0);
      String result = describeResult();
      String retrieveType = doUseDirectSql ? "SQL" : "ORM";

      LOG.debug("{} retrieved using {} in {}ms", result, retrieveType, time);
    }
    return results;
  }

  private void close() {
    if (!success) {
      baseStore.rollbackTransaction();
    }
  }

  public Table getTable() {
    return table;
  }

  public MetaStoreDirectSql getDirectSql() {
    return directSql;
  }

  public List<String> getPartitionFields() {
    return partitionFields;
  }

  public static long getDirectSqlErrors() {
    return directSqlErrors.getCount();
  }
}