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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;
import java.util.Map;

/**
 * TestReplicationScenariosAcidTables - test bootstrap of ACID tables during an incremental.
 */
public class TestReplicationScenariosAcidTablesBootstrap
        extends BaseReplicationScenariosAcidTables {

  private static List<String> dumpWithoutAcidClause = Collections.singletonList(
          "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'");
  private static List<String> dumpWithAcidBootstrapClause = Arrays.asList(
          "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'",
          "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES + "'='true'");

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, TestReplicationScenariosAcidTablesBootstrap.class);
  }

  @Test
  public void testAcidTablesBootstrapDuringIncremental() throws Throwable {
    // Take a bootstrap dump without acid tables
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
            dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, false);

    // Take a incremental dump with acid table bootstrap
    prepareIncAcidData(primaryDbName);
    prepareIncNonAcidData(primaryDbName);
    LOG.info(testName.getMethodName() + ": incremental dump and load dump with acid table bootstrap.");
    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
    replica.load(replicatedDbName, incrementalDump.dumpLocation);
    verifyIncLoad(replicatedDbName, incrementalDump.lastReplicationId);
    // Ckpt should be set on bootstrapped tables.
    replica.verifyIfCkptSetForTables(replicatedDbName, acidTableNames, incrementalDump.dumpLocation);

    // Take a second normal incremental dump after Acid table boostrap
    prepareInc2AcidData(primaryDbName, primary.hiveConf);
    prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
    LOG.info(testName.getMethodName()
             + ": second incremental dump and load dump after incremental with acid table " +
            "bootstrap.");
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, incrementalDump.lastReplicationId);
    replica.load(replicatedDbName, inc2Dump.dumpLocation);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void testRetryAcidTablesBootstrapFromDifferentDump() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
            dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, false);

    prepareIncAcidData(primaryDbName);
    prepareIncNonAcidData(primaryDbName);
    LOG.info(testName.getMethodName() + ": first incremental dump with acid table bootstrap.");
    WarehouseInstance.Tuple incDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);

    // Fail setting ckpt property for table t5 but success for earlier tables
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (args.tblName.equalsIgnoreCase("t5") && args.dbName.equalsIgnoreCase(replicatedDbName)) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB : " + args.dbName + " TABLE : " + args.tblName);
          return false;
        }
        return true;
      }
    };

    // Fail repl load before the ckpt property is set for t4 and after it is set for t2.
    // In the retry, these half baked tables should be dropped and bootstrap should be successful.
    InjectableBehaviourObjectStore.setAlterTableModifier(callerVerifier);
    try {
      LOG.info(testName.getMethodName()
              + ": loading first incremental dump with acid table bootstrap (will fail)");
      replica.loadFailure(replicatedDbName, incDump.dumpLocation);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetAlterTableModifier();
    }

    prepareInc2AcidData(primaryDbName, primary.hiveConf);
    prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
    LOG.info(testName.getMethodName() + ": second incremental dump with acid table bootstrap");
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);

    // Set incorrect bootstrap dump to clean tables. Here, used the full bootstrap dump which is invalid.
    // So, REPL LOAD fails.
    List<String> loadWithClause = Collections.singletonList(
            "'" + ReplUtils.REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
            + bootstrapDump.dumpLocation + "'");
    LOG.info(testName.getMethodName()
            + ": trying to load second incremental dump with wrong bootstrap dump "
            + " specified for cleaning ACID tables. Should fail.");
    replica.loadFailure(replicatedDbName, inc2Dump.dumpLocation, loadWithClause);

    // Set previously failed bootstrap dump to clean-up. Now, new bootstrap should overwrite the old one.
    loadWithClause = Collections.singletonList(
            "'" + ReplUtils.REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
                    + incDump.dumpLocation + "'");

    LOG.info(testName.getMethodName()
            + ": trying to load second incremental dump with correct bootstrap dump "
            + "specified for cleaning ACID tables. Should succeed.");
    replica.load(replicatedDbName, inc2Dump.dumpLocation, loadWithClause);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);

    // Once the REPL LOAD is successful, the this config should be unset or else, the subsequent REPL LOAD
    // will also drop those tables which will cause data loss.
    loadWithClause = Collections.emptyList();

    // Verify if bootstrapping with same dump is idempotent and return same result
    LOG.info(testName.getMethodName()
            + ": trying to load second incremental dump (with acid bootstrap) again."
            + " Should succeed.");
    replica.load(replicatedDbName, inc2Dump.dumpLocation, loadWithClause);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void retryIncBootstrapAcidFromDifferentDumpWithoutCleanTablesConfig() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
            dumpWithoutAcidClause);
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);

    prepareIncAcidData(primaryDbName);
    prepareIncNonAcidData(primaryDbName);
    WarehouseInstance.Tuple incDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
    replica.load(replicatedDbName, incDump.dumpLocation);

    // Re-bootstrapping from different bootstrap dump without clean tables config should fail.
    replica.loadFailure(replicatedDbName, inc2Dump.dumpLocation, Collections.emptyList(),
            ErrorMsg.REPL_BOOTSTRAP_LOAD_PATH_NOT_VALID.getErrorCode());
  }

  @Test
  public void testAcidTablesBootstrapDuringIncrementalWithOpenTxnsTimeout() throws Throwable {
    // Take a dump without ACID tables
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
                                                                dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);

    // Open concurrent transactions, create data for incremental and take an incremental dump
    // with ACID table bootstrap.
    int numTxns = 5;
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());
    // Open 5 txns
    List<Long> txns = openTxns(numTxns, txnHandler, primaryConf);
    prepareIncNonAcidData(primaryDbName);
    prepareIncAcidData(primaryDbName);
    // Allocate write ids for tables t1 and t2 for all txns
    // t1=5+2(insert) and t2=5+5(insert, alter add column)
    Map<String, Long> tables = new HashMap<>();
    tables.put("t1", numTxns+2L);
    tables.put("t2", numTxns+5L);
    allocateWriteIdsForTables(primaryDbName, tables, txnHandler, txns, primaryConf);

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = new LinkedList<>(dumpWithAcidBootstrapClause);
            withConfigs.add("'hive.repl.bootstrap.dump.open.txn.timeout'='1s'");
    WarehouseInstance.Tuple incDump = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, withConfigs);

    // After bootstrap dump, all the opened txns should be aborted. Verify it.
    verifyAllOpenTxnsAborted(txns, primaryConf);
    verifyNextId(tables, primaryDbName, primaryConf);

    // Incremental load with ACID bootstrap should also replicate the aborted write ids on
    // tables t1 and t2
    HiveConf replicaConf = replica.getConf();
    LOG.info(testName.getMethodName() + ": loading incremental dump with ACID bootstrap.");
    replica.load(replicatedDbName, incDump.dumpLocation);
    verifyIncLoad(replicatedDbName, incDump.lastReplicationId);
    // Verify if HWM is properly set after REPL LOAD
    verifyNextId(tables, replicatedDbName, replicaConf);

    // Verify if all the aborted write ids are replicated to the replicated DB
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      entry.setValue((long) numTxns);
    }
    verifyWriteIdsForTables(tables, replicaConf, replicatedDbName);

    // Verify if entries added in COMPACTION_QUEUE for each table/partition
    // t1-> 1 entry and t2-> 2 entries (1 per partition)
    tables.clear();
    tables.put("t1", 1L);
    tables.put("t2", 4L);
    verifyCompactionQueue(tables, replicatedDbName, replicaConf);
  }

  @Test
  public void testBootstrapAcidTablesDuringIncrementalWithConcurrentWrites() throws Throwable {
    // Dump and load bootstrap without ACID tables.
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
                                                                dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);

    // Create incremental data for incremental load with bootstrap of ACID
    prepareIncNonAcidData(primaryDbName);
    prepareIncAcidData(primaryDbName);
    // Perform concurrent writes. Bootstrap won't see the written data but the subsequent
    // incremental repl should see it. We can not inject callerVerifier since an incremental dump
    // would not cause an ALTER DATABASE event. Instead we piggy back on
    // getCurrentNotificationEventId() which is anyway required for a bootstrap.
    BehaviourInjection<CurrentNotificationEventId, CurrentNotificationEventId> callerInjectedBehavior
            = new BehaviourInjection<CurrentNotificationEventId, CurrentNotificationEventId>() {
      @Nullable
      @Override
      public CurrentNotificationEventId apply(@Nullable CurrentNotificationEventId input) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // Do some writes through concurrent thread
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              try {
                prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
                prepareInc2AcidData(primaryDbName, primary.hiveConf);
              } catch (Throwable t) {
                Assert.assertNull(t);
              }
              LOG.info("Exit new thread success");
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return input;
      }
    };

    InjectableBehaviourObjectStore.setGetCurrentNotificationEventIdBehaviour(callerInjectedBehavior);
    WarehouseInstance.Tuple incDump = null;
    try {
      incDump = primary.dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      // reset the behaviour
      InjectableBehaviourObjectStore.resetGetCurrentNotificationEventIdBehaviour();
    }

    // While bootstrapping ACID tables it has taken snapshot before concurrent thread performed
    // write. So concurrent writes won't be dumped.
    LOG.info(testName.getMethodName() +
            ": loading incremental dump containing bootstrapped ACID tables.");
    replica.load(replicatedDbName, incDump.dumpLocation);
    verifyIncLoad(replicatedDbName, incDump.lastReplicationId);

    // Next Incremental should include the concurrent writes
    LOG.info(testName.getMethodName() +
            ": dumping second normal incremental dump from event id = " + incDump.lastReplicationId);
    WarehouseInstance.Tuple inc2Dump = primary.dump(primaryDbName, incDump.lastReplicationId);
    LOG.info(testName.getMethodName() +
            ": loading second normal incremental dump from event id = " + incDump.lastReplicationId);
    replica.load(replicatedDbName, inc2Dump.dumpLocation);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }
}
