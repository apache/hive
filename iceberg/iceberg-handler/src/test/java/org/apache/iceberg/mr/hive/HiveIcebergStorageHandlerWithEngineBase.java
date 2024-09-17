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
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assume.assumeTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class HiveIcebergStorageHandlerWithEngineBase {

  protected static final String[] EXECUTION_ENGINES = new String[] {"tez"};

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

  @Parameters(name = "fileFormat={0}, engine={1}, catalog={2}, isVectorized={3}, formatVersion={4}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = Lists.newArrayList();
    String javaVersion = System.getProperty("java.specification.version");

    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      for (String engine : EXECUTION_ENGINES) {
        IntStream.of(2, 1).forEach(formatVersion -> {
          // include Tez tests only for Java 8
          if (javaVersion.equals("1.8")) {
            testParams.add(new Object[]{fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, false,
                formatVersion});
            // test for vectorization=ON in case of ORC and PARQUET format with Tez engine
            if (fileFormat != FileFormat.METADATA && "tez".equals(engine) && HiveVersion.min(HiveVersion.HIVE_3)) {
              testParams.add(new Object[]{fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, true,
                  formatVersion});
            }
          }
        });
      }
    }

    // Run tests for every Catalog for a single FileFormat (PARQUET) and execution engine (tez)
    // skip HiveCatalog tests as they are added before
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      if (!TestTables.TestTableType.HIVE_CATALOG.equals(testTableType)) {
        testParams.add(new Object[]{FileFormat.PARQUET, "tez", testTableType, false, 1});
      }
    }

    return testParams;
  }

  protected static TestHiveShell shell;

  protected TestTables testTables;

  @Parameter(0)
  public FileFormat fileFormat;

  @Parameter(1)
  public String executionEngine;

  @Parameter(2)
  public TestTables.TestTableType testTableType;

  @Parameter(3)
  public boolean isVectorized;

  @Parameter(4)
  public Integer formatVersion;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(500_000, TimeUnit.MILLISECONDS);

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
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, executionEngine);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
    // Fetch task conversion might kick in for certain queries preventing vectorization code path to be used, so
    // we turn it off explicitly to achieve better coverage.
    if (isVectorized) {
      HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    } else {
      HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "more");
    }
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
}
