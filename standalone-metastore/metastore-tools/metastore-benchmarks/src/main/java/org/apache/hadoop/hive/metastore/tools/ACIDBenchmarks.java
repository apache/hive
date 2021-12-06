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

package org.apache.hadoop.hive.metastore.tools;

import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.tools.BenchmarkUtils.createManyTables;
import static org.apache.hadoop.hive.metastore.tools.BenchmarkUtils.dropManyTables;
import static org.apache.hadoop.hive.metastore.tools.Util.throwingSupplierWrapper;

public class ACIDBenchmarks {

  private static final Logger LOG = LoggerFactory.getLogger(CoreContext.class);

  @State(Scope.Benchmark)
  public static class CoreContext {
    @Param("1")
    protected int howMany;

    @State(Scope.Thread)
    public static class ThreadState {
      HMSClient client;

      @Setup
      public void doSetup() throws Exception {
        LOG.debug("Creating client");
        client = HMSConfig.getInstance().newClient();
      }

      @TearDown
      public void doTearDown() throws Exception {
        client.close();
        LOG.debug("Closed a connection to metastore.");
      }
    }

    @Setup
    public void setup() {
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      Configuration ctxConfig = ctx.getConfiguration();
      ctxConfig.getLoggerConfig(CoreContext.class.getName()).setLevel(Level.INFO);
      ctx.updateLoggers(ctxConfig);
    }
  }

  @State(Scope.Benchmark)
  public static class TestOpenTxn extends CoreContext {

    @State(Scope.Thread)
    public static class ThreadState extends CoreContext.ThreadState {
      List<Long> openTxns = new ArrayList<>();

      @TearDown
      public void doTearDown() throws Exception {
        client.abortTxns(openTxns);
        LOG.debug("aborted all opened txns");
      }

      void addTxn(List<Long> openTxn) {
        openTxns.addAll(openTxn);
      }
    }

    @Benchmark
    public void openTxn(TestOpenTxn.ThreadState state) throws TException {
      state.addTxn(state.client.openTxn(howMany));
      LOG.debug("opened txns, count=", howMany);
    }
  }

  @State(Scope.Benchmark)
  public static class TestLocking extends CoreContext {
    private int nTables;

    @Param("0")
    private int nPartitions;

    private List<LockComponent> lockComponents;

    @Setup
    public void setup() {
      this.nTables = (nPartitions != 0) ? howMany / nPartitions : howMany;
      createLockComponents();
    }

    @State(Scope.Thread)
    public static class ThreadState extends CoreContext.ThreadState {
      List<Long> openTxns = new ArrayList<>();
      long txnId;

      @Setup(org.openjdk.jmh.annotations.Level.Invocation)
      public void iterSetup() {
        txnId = executeOpenTxnAndGetTxnId(client);
        LOG.debug("opened txn, id={}", txnId);
        openTxns.add(txnId);
      }

      @TearDown
      public void doTearDown() throws Exception {
        client.abortTxns(openTxns);
        if (BenchmarkUtils.checkTxnsCleaned(client, openTxns) == false) {
          LOG.error("Something went wrong with the cleanup of txns");
        }
        LOG.debug("aborted all opened txns");
      }
    }

    @Benchmark
    public void lock(TestLocking.ThreadState state) {
      LOG.debug("sending lock request");
      executeLock(state.client, state.txnId, lockComponents);
    }

    private void createLockComponents() {
      lockComponents = new ArrayList<>();

      for (int i = 0; i < nTables; i++) {
        for (int j = 0; j < nPartitions - (nPartitions > 1 ? 1 : 0); j++) {
          lockComponents.add(
            new Util.LockComponentBuilder()
              .setDbName("default")
              .setTableName(String.format("tmp_table_%d", i))
              .setPartitionName("p_" + j)
              .setShared()
              .setOperationType(DataOperationType.SELECT)
              .build());
        }
        if (nPartitions != 1) {
          lockComponents.add(
            new Util.LockComponentBuilder()
              .setDbName("default")
              .setTableName(String.format("tmp_table_%d", i))
              .setShared()
              .setOperationType(DataOperationType.SELECT)
              .build());
        }
      }
    }

    private static long executeOpenTxnAndGetTxnId(HMSClient client) {
      return throwingSupplierWrapper(() -> client.openTxn(1).get(0));
    }

    private void executeLock(HMSClient client, long txnId, List<LockComponent> lockComponents) {
      LockRequest req = new LockRequest(lockComponents, "hclient", "localhost");
      req.setTxnid(txnId);
      throwingSupplierWrapper(() -> client.lock(req));
    }
  }

  @State(Scope.Benchmark)
  public static class TestAllocateTableWriteIds extends CoreContext {
    String dbName = "test_db";
    String tblName = "tmp_table";

    @State(Scope.Thread)
    public static class ThreadState extends CoreContext.ThreadState {
      List<Long> openTxns = new ArrayList<>();
      long txnId;

      @Setup
      public void iterSetup() {
        txnId = executeOpenTxnAndGetTxnId(client);
        LOG.info("opened txn, id={}", txnId);
        openTxns.add(txnId);
      }

      @TearDown
      public void doTearDown() throws Exception {
        client.abortTxns(openTxns);
        if (BenchmarkUtils.checkTxnsCleaned(client, openTxns) == false) {
          LOG.error("Something went wrong with the cleanup of txns");
        }
        LOG.info("aborted all opened txns");
      }
    }

    @Benchmark
    public void allocateTableWriteIds(TestAllocateTableWriteIds.ThreadState state) throws TException {
      state.client.allocateTableWriteIds(dbName, tblName, state.openTxns);
    }

    private static long executeOpenTxnAndGetTxnId(HMSClient client) {
      return throwingSupplierWrapper(() -> client.openTxn(1).get(0));
    }
  }

  @State(Scope.Benchmark)
  public static class TestGetValidWriteIds extends CoreContext {
    String dbName = "test_db";
    String tblName = "table_%d";
    List<String> fullTableNames = new ArrayList<>();
    HMSClient client;

    @Setup
    public void doSetup() {
      try {
        client = HMSConfig.getInstance().newClient();
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }

      try {
        if (!client.dbExists(dbName)) {
          client.createDatabase(dbName);
        }
      } catch (TException e) {
        LOG.error(e.getMessage());
      }

      LOG.info("creating {} tables", this.howMany);
      createManyTables(client, this.howMany, dbName, tblName);
      for (int i = 0; i < this.howMany; i++) {
        fullTableNames.add(dbName + ".table_" + i);
      }
    }

    @TearDown
    public void doTearDown() throws Exception {
      LOG.debug("dropping {} tables", howMany);
      dropManyTables(client, howMany, dbName, tblName);
    }

    @Benchmark
    public void getValidWriteIds(TestGetValidWriteIds.ThreadState state) throws TException {
      LOG.debug("executing getValidWriteIds");
      state.client.getValidWriteIds(this.fullTableNames);
    }
  }
}

