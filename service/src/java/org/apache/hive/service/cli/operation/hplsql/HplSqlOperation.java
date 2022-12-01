/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hive.service.cli.operation.hplsql;

import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hplsql.Arguments;
import org.apache.hive.hplsql.Exec;
import org.apache.hive.hplsql.ResultListener;
import org.apache.hive.hplsql.executor.Metadata;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.server.ThreadWithGarbageCleanup;

import static org.apache.hadoop.hive.shims.HadoopShims.USER_ID;

public class HplSqlOperation extends ExecuteStatementOperation implements ResultListener {
  private final Exec exec;
  private final boolean runInBackground;
  private RowSet rowSet;
  private TableSchema schema;

  public HplSqlOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay, boolean runInBackground, Exec exec) {
    super(parentSession, statement, confOverlay, runInBackground, false);
    this.exec = exec;
    this.runInBackground = runInBackground;
    this.exec.setResultListener(this);
  }

  @Override
  protected void runInternal() throws HiveSQLException {
    setState(OperationState.PENDING);
    if (!runInBackground) {
      interpret();
    } else {
      Runnable work = new BackgroundWork(getCurrentUGI(), parentSession.getSessionHive(), SessionState.get());
      try {
        // This submit blocks if no background threads are available to run this operation
        Future<?> backgroundHandle = getParentSession().submitBackgroundOperation(work);
        setBackgroundHandle(backgroundHandle);
      } catch (RejectedExecutionException rejected) {
        setState(OperationState.ERROR);
        throw new HiveSQLException("The background threadpool cannot accept" +
                " new task for execution, please retry the operation", rejected);
      }
    }
  }

  private void interpret() throws HiveSQLException {
    try {
      OperationState opState = getState();
      // Operation may have been cancelled by another thread
      if (opState.isTerminal()) {
        log.info("Not running the query. Operation is already in terminal state: " + opState
                + ", perhaps cancelled due to query timeout or by another thread.");
        return;
      }
      setState(OperationState.RUNNING);
      Arguments args = new Arguments();
      args.parse(new String[]{"-e", statement});
      exec.parseAndEval(args);
      setState(OperationState.FINISHED);
    } catch (Throwable e) {
      if (getState().isTerminal()) {
        log.warn("Ignore exception in terminal state: {}", getState(), e);
        return;
      }
      setState(OperationState.ERROR);
      if (e instanceof HiveSQLException) {
        throw (HiveSQLException) e;
      } else if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      } else {
        throw new HiveSQLException("Error running HPL/SQL operation", e);
      }
    } finally {
      exec.printExceptions();
    }
  }

  @Override
  public void cancel(OperationState stateAfterCancel) {
    throw new UnsupportedOperationException("HplSqlOperation.cancel()");
  }

  @Override
  public void close() throws HiveSQLException {
    setState(OperationState.CLOSED);
    cleanupOperationLog(0);
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(Collections.singleton(OperationState.FINISHED));
    return schema;
  }

  private static TableSchema convertToTableSchema(Metadata metadata) {
    TableSchema tableSchema = new TableSchema(metadata.columnCount());
    for (int i = 0; i < metadata.columnCount(); i++) {
      tableSchema.addPrimitiveColumn(
              metadata.columnName(i),
              Type.fromJavaSQLType(metadata.jdbcType(i)),
              null);
    }
    return tableSchema;
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(Collections.singleton(OperationState.FINISHED));
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows);
  }

  @Override
  public OperationType getType() {
    return OperationType.PROCEDURAL_SQL;
  }

  @Override
  public boolean shouldRunAsync() {
    return runInBackground;
  }

  @Override
  public void onRow(Object[] rows) {
    rowSet.addRow(rows);
  }

  @Override
  public void onMetadata(Metadata metadata) {
    this.schema = convertToTableSchema(metadata);
    this.rowSet = RowSetFactory.create(schema, getProtocolVersion(), false);
    setHasResultSet(true);
  }

  private final class BackgroundWork implements Runnable {
    private final UserGroupInformation currentUGI;
    private final Hive parentHive;
    private final SessionState parentSessionState;

    private BackgroundWork(UserGroupInformation currentUGI,
                           Hive parentHive,
                           SessionState parentSessionState) {
      // Note: parentHive can be shared by multiple threads and so it should be protected from any
      // thread closing metastore connections when some other thread still accessing it. So, it is
      // expected that allowClose flag in parentHive is set to false by caller and it will be caller's
      // responsibility to close it explicitly with forceClose flag as true.
      // Shall refer to sessionHive in HiveSessionImpl.java for the usage.
      this.currentUGI = currentUGI;
      this.parentHive = parentHive;
      this.parentSessionState = parentSessionState;
    }

    @Override
    public void run() {
      PrivilegedExceptionAction<Object> doAsAction = () -> {
        assert (!parentHive.allowClose());
        Hive.set(parentHive);
        SessionState.setCurrentSessionState(parentSessionState);
        PerfLogger.setPerfLogger(SessionState.getPerfLogger());
        LogUtils.registerLoggingContext(queryState.getConf());
        ShimLoader.getHadoopShims()
            .setHadoopQueryContext(String.format(USER_ID, queryState.getQueryId(), parentSessionState.getUserName()));
        try {
          interpret();
        } catch (HiveSQLException e) {
          setOperationException(e);
          log.error("Error running hive query", e);
        } finally {
          LogUtils.unregisterLoggingContext();
          Hive.closeCurrent();
        }
        return null;
      };

      try {
        currentUGI.doAs(doAsAction);
      } catch (Exception e) {
        setOperationException(new HiveSQLException(e));
        log.error("Error running hive query as user : {}", currentUGI.getShortUserName(), e);
      } finally {
          /**
         * We'll cache the ThreadLocal RawStore object for this background thread for an orderly cleanup
         * when this thread is garbage collected later.
         * @see org.apache.hive.service.server.ThreadWithGarbageCleanup#finalize()
         */
        if (ThreadWithGarbageCleanup.currentThread() instanceof ThreadWithGarbageCleanup) {
          ThreadWithGarbageCleanup currentThread =
                  (ThreadWithGarbageCleanup) ThreadWithGarbageCleanup.currentThread();
          currentThread.cacheThreadLocalRawStore();
        }
      }
    }
  }

  private UserGroupInformation getCurrentUGI() throws HiveSQLException {
    try {
      return Utils.getUGI();
    } catch (Exception e) {
      throw new HiveSQLException("Unable to get current user", e);
    }
  }
}
