/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.test.TestHiveShell;
import org.apache.iceberg.mr.hive.test.TestTables;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.concurrent.HiveIcebergStorageHandlerStub;
import org.apache.iceberg.mr.hive.test.concurrent.TestUtilPhaser;
import org.apache.iceberg.mr.hive.test.concurrent.WithMockedStorageHandler;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.mr.hive.test.TestTables.ALL_TABLE_TYPES;
import static org.apache.iceberg.mr.hive.test.TestTables.TestTableType.HIVE_CATALOG;
import static org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils.init;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assume.assumeTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class HiveIcebergStorageHandlerWithEngineBase {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergStorageHandlerWithEngineBase.class);

  public static final String RETRY_STRATEGIES =
      "overlay,reoptimize,reexecute_lost_am,dagsubmit,recompile_without_cbo,write_conflict";

  public static final String RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT =
      "overlay,reoptimize,reexecute_lost_am," + "dagsubmit,recompile_without_cbo";

  protected static final Schema ORDER_SCHEMA = new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()),
          required(4, "product_id", Types.LongType.get())
  );

  protected static final List<Record> ORDER_RECORDS = TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d, 1L)
          .add(101L, 0L, 22.22d, 2L)
          .add(102L, 1L, 33.33d, 3L)
          .build();

  protected static final Schema PRODUCT_SCHEMA = new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "price", Types.DoubleType.get())
  );

  protected static final List<Record> PRODUCT_RECORDS = TestHelper.RecordsBuilder.newInstance(PRODUCT_SCHEMA)
          .add(1L, "skirt", 11.11d)
          .add(2L, "tee", 22.22d)
          .add(3L, "watch", 33.33d)
          .build();

  protected static final List<Type> SUPPORTED_TYPES =
          ImmutableList.of(Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
                  Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimestampType.withZone(),
                  Types.TimestampType.withoutZone(), Types.StringType.get(), Types.BinaryType.get(),
                  Types.DecimalType.of(3, 1), Types.UUIDType.get(), Types.FixedType.ofLength(5),
                  Types.TimeType.get());

  protected static final Map<String, String> STATS_MAPPING = ImmutableMap.of(
      StatsSetupConst.NUM_FILES, SnapshotSummary.TOTAL_DATA_FILES_PROP,
      StatsSetupConst.ROW_COUNT, SnapshotSummary.TOTAL_RECORDS_PROP,
      StatsSetupConst.TOTAL_SIZE, SnapshotSummary.TOTAL_FILE_SIZE_PROP
  );

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = Lists.newArrayList();

    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      IntStream.rangeClosed(1, 3).forEach(formatVersion -> {
        testParams.add(new Object[]{fileFormat, HIVE_CATALOG, false, formatVersion});
        // test for vectorization=ON in case of ORC and PARQUET format
        if (fileFormat != FileFormat.METADATA) {
          testParams.add(new Object[]{fileFormat, HIVE_CATALOG, true, formatVersion});
        }
      });
    }

    // Run tests for every Catalog for a single FileFormat (PARQUET), skip HiveCatalog tests as they are added before
    for (TestTableType testTableType : ALL_TABLE_TYPES) {
      if (testTableType != HIVE_CATALOG) {
        testParams.add(new Object[]{FileFormat.PARQUET, testTableType, false, 1});
      }
    }

    return testParams;
  }

  protected static TestHiveShell shell;

  protected TestTables testTables;

  @Parameter(0)
  public FileFormat fileFormat;

  @Parameter(1)
  public TestTableType testTableType;

  @Parameter(2)
  public boolean isVectorized;

  @Parameter(3)
  public Integer formatVersion;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(500_000, TimeUnit.MILLISECONDS);

  @Rule
  public WithMockedStorageHandler.Rule mockedStorageHandlerRule = new WithMockedStorageHandler.Rule();

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    validateTestParams();
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
    // Fetch task conversion might kick in for certain queries preventing vectorization code path to be used, so
    // we turn it off explicitly to achieve better coverage.
    HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION,
        isVectorized ? "none" : "more");
  }

  protected void validateTestParams() {
    assumeTrue(formatVersion == 1);
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
    // Mixing mr and tez jobs within the same JVM can cause problems. Mr jobs set the ExecMapper status to done=false
    // at the beginning and to done=true at the end. However, tez jobs also rely on this value to see if they should
    // proceed, but they do not reset it to done=false at the beginning. Therefore, without calling this after each test
    // case, any tez job that follows a completed mr job will erroneously read done=true and will not proceed.
    ExecMapper.setDone(false);
  }

  protected void validateBasicStats(Table icebergTable, String dbName, String tableName)
      throws TException, InterruptedException {
    Map<String, String> hmsParams = shell.metastore().getTable(dbName, tableName).getParameters();
    Map<String, String> summary = Maps.newHashMap();
    if (icebergTable.currentSnapshot() == null) {
      for (String key : STATS_MAPPING.values()) {
        summary.put(key, "0");
      }
    } else {
      summary = icebergTable.currentSnapshot().summary();
    }

    for (Map.Entry<String, String> entry : STATS_MAPPING.entrySet()) {
      Assert.assertEquals(summary.get(entry.getValue()), hmsParams.get(entry.getKey()));
    }
  }

  /**
   * Executes multiple SQL queries concurrently with controlled synchronization for testing.
   *
   * <p>This method supports two synchronization modes:
   * <ul>
   *   <li><b>Ext-locking mode</b> ({@code useExtLocking=true}): Queries execute in strict sequential
   *       order (sql[0], then sql[1], then sql[2], ...) to verify that external table locking prevents
   *       concurrent execution. Each query waits for its turn before starting.</li>
   *   <li><b>Barrier synchronization mode</b> ({@code useExtLocking=false}): Queries execute concurrently,
   *       then commit in order to test optimistic concurrency control and retry on write conflicts.</li>
   * </ul>
   *
   * <p>Uses {@link TestUtilPhaser} and {@link HiveIcebergStorageHandlerStub} to coordinate thread
   * execution order deterministically.
   *
   * @param useExtLocking if true, enables external locking mode with sequential execution;
   *                      if false, enables concurrent execution with ordered commits
   * @param retryStrategies comma-separated list of Hive query retry strategies to enable
   * @param sql array of SQL queries to execute. If single query provided, it's executed by all threads.
   *            If multiple queries provided, each thread executes sql[i] where i is the thread index.
   * @throws Exception if any query execution fails
   */
  protected void executeConcurrently(
      boolean useExtLocking, String retryStrategies, String... sql) throws Exception {

    int nThreads = sql.length > 1 ? sql.length : 2;
    TestUtilPhaser testUtilPhaser = TestUtilPhaser.getInstance();

    try (ExecutorService executor =
             Executors.newVirtualThreadPerTaskExecutor()) {
      Tasks.range(nThreads)
          .executeWith(executor)
          .run(i -> {
            LOG.debug("Thread {} started for query index {}", Thread.currentThread().getName(), i);

            TestUtilPhaser.ThreadContext.setQueryIndex(i);

            if (useExtLocking) {
              TestUtilPhaser.ThreadContext.setUseExtLocking(true);
            } else {
              testUtilPhaser.register();
            }

            init(shell, testTables, temp);
            HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
            HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_DELETE, false);
            HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
                retryStrategies);

            if (useExtLocking) {
              HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED, true);
              shell.getHiveConf().setBoolean(ConfigProperties.LOCK_HIVE_ENABLED, false);

              // Wait for turn: sql[0] waits for phase 0 -> 1, sql[1] for 1 -> 2, etc.
              // Phase advances when previous query validates snapshot.
              LOG.debug("Thread {} (queryIndex={}) waiting for turn",
                  Thread.currentThread().getName(), i);
              testUtilPhaser.awaitTurn();
            }

            try {
              shell.executeStatement(sql.length > 1 ? sql[i] : sql[0]);
              LOG.debug("Thread {} (queryIndex={}) completed statement execution",
                  Thread.currentThread().getName(), i);
            } finally {
              shell.closeSession();
            }
          });
    } catch (Exception ex) {
      Throwable root = Throwables.getRootCause(ex);
      if (root instanceof Exception) {
        throw (Exception) root;
      }
      throw new RuntimeException(root);
    } finally {
      TestUtilPhaser.destroyInstance();
    }
  }
}
