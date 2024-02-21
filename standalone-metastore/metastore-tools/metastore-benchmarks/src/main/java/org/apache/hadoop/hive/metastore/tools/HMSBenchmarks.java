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

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.PartitionManagementTask;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hive.metastore.tools.Util.addManyPartitions;
import static org.apache.hadoop.hive.metastore.tools.Util.addManyPartitionsNoException;
import static org.apache.hadoop.hive.metastore.tools.Util.createManyPartitions;
import static org.apache.hadoop.hive.metastore.tools.Util.createSchema;
import static org.apache.hadoop.hive.metastore.tools.Util.throwingSupplierWrapper;

/**
 * Actual benchmark code.
 */
final class HMSBenchmarks {
  private static final Logger LOG = LoggerFactory.getLogger(HMSBenchmarks.class);

  private static final String PARAM_KEY = "parameter_";
  private static final String PARAM_VALUE = "value_";

  static DescriptiveStatistics benchmarkListDatabases(@NotNull MicroBenchmark benchmark,
                                                      @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    return benchmark.measure(() ->
        throwingSupplierWrapper(() -> client.getAllDatabases(null)));
  }

  static DescriptiveStatistics benchmarkListAllTables(@NotNull MicroBenchmark benchmark,
                                                      @NotNull BenchData data) {

    final HMSClient client = data.getClient();
    String dbName = data.dbName;

    return benchmark.measure(() ->
        throwingSupplierWrapper(() -> client.getAllTables(dbName, null)));
  }

  static DescriptiveStatistics benchmarkTableCreate(@NotNull MicroBenchmark bench,
                                                    @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Table table = Util.TableBuilder.buildDefaultTable(dbName, tableName);

    return bench.measure(null,
        () -> throwingSupplierWrapper(() -> client.createTable(table)),
        () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName)));
  }

  static DescriptiveStatistics benchmarkDeleteCreate(@NotNull MicroBenchmark bench,
                                                     @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Table table = Util.TableBuilder.buildDefaultTable(dbName, tableName);

    return bench.measure(
        () -> throwingSupplierWrapper(() -> client.createTable(table)),
        () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName)),
        null);
  }

  static DescriptiveStatistics benchmarkDeleteWithPartitions(@NotNull MicroBenchmark bench,
                                                             @NotNull BenchData data,
                                                             int howMany,
                                                             int nparams) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    // Create many parameters
    Map<String, String> parameters = new HashMap<>(nparams);
    for (int i = 0; i < nparams; i++) {
      parameters.put(PARAM_KEY + i, PARAM_VALUE + i);
    }

    return bench.measure(
        () -> throwingSupplierWrapper(() -> {
          BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
          addManyPartitions(client, dbName, tableName, parameters,
              Collections.singletonList("d"), howMany);
          return true;
        }),
        () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName)),
        null);
  }

  static DescriptiveStatistics benchmarkDeleteMetaOnlyWithPartitions(@NotNull MicroBenchmark bench,
                                                                        @NotNull BenchData data,
                                                                        int howMany,
                                                                        int nparams) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    // Create many parameters
    Map<String, String> parameters = new HashMap<>(nparams);
    for (int i = 0; i < nparams; i++) {
      parameters.put(PARAM_KEY + i, PARAM_VALUE + i);
    }

    return bench.measure(
            () -> throwingSupplierWrapper(() -> {
              BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
              addManyPartitions(client, dbName, tableName, parameters,
                      Collections.singletonList("d"), howMany);
              return true;
            }),
            () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName, false)),
            null);
  }

  static DescriptiveStatistics benchmarkGetTable(@NotNull MicroBenchmark bench,
                                                 @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      return bench.measure(() ->
          throwingSupplierWrapper(() -> client.getTable(dbName, tableName)));
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkListTables(@NotNull MicroBenchmark bench,
                                                   @NotNull BenchData data,
                                                   int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;

    // Create a bunch of tables
    String format = "tmp_table_%d";
    try {
      BenchmarkUtils.createManyTables(client, count, dbName, format);
      return bench.measure(() ->
          throwingSupplierWrapper(() -> client.getAllTables(dbName, null)));
    } finally {
      BenchmarkUtils.dropManyTables(client, count, dbName, format);
    }
  }

  static DescriptiveStatistics benchmarkCreatePartition(@NotNull MicroBenchmark bench,
                                                        @NotNull BenchData data,
                                                        int howMany) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      Table t = client.getTable(dbName, tableName);
      List<Partition> parts = createManyPartitions(t, null, Collections.singletonList("d"), howMany);

      return bench.measure(null,
          () -> throwingSupplierWrapper(() -> {
            parts.forEach(part -> throwingSupplierWrapper(() -> client.addPartition(part)));
            return null;
          }),
          () -> throwingSupplierWrapper(() -> client.dropPartitions(dbName, tableName, null)));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkListPartition(@NotNull MicroBenchmark bench,
                                                      @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      addManyPartitions(client, dbName, tableName, null,
          Collections.singletonList("d"), 1);

      return bench.measure(() ->
          throwingSupplierWrapper(() -> client.listPartitions(dbName, tableName)));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkListManyPartitions(@NotNull MicroBenchmark bench,
                                                           @NotNull BenchData data,
                                                           int howMany) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      addManyPartitions(client, dbName, tableName, null, Collections.singletonList("d"), howMany);
      LOG.debug("Created {} partitions", howMany);
      LOG.debug("started benchmark... ");
      return bench.measure(() ->
          throwingSupplierWrapper(() -> client.listPartitions(dbName, tableName)));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkGetPartitions(@NotNull MicroBenchmark bench,
                                                      @NotNull BenchData data,
                                                      int howMany) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      addManyPartitions(client, dbName, tableName, null, Collections.singletonList("d"), howMany);
      LOG.debug("Created {} partitions", howMany);
      LOG.debug("started benchmark... ");
      return bench.measure(() ->
          throwingSupplierWrapper(() -> client.getPartitions(dbName, tableName)));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkDropPartition(@NotNull MicroBenchmark bench,
                                                      @NotNull BenchData data,
                                                      int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      return bench.measure(
          () -> addManyPartitionsNoException(client, dbName, tableName, null,
                  Collections.singletonList("d"), count),
          () -> throwingSupplierWrapper(() -> {
            List<String> partNames = client.getPartitionNames(dbName, tableName);
            partNames.forEach(partName ->
                throwingSupplierWrapper(() -> client.dropPartition(dbName, tableName, partName)));
            return null;
          }),
          null);
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkCreatePartitions(@NotNull MicroBenchmark bench,
                                                         @NotNull BenchData data,
                                                         int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      return bench.measure(
          null,
          () -> addManyPartitionsNoException(client, dbName, tableName, null,
              Collections.singletonList("d"), count),
          () -> throwingSupplierWrapper(() ->
              client.dropPartitions(dbName, tableName, null))
      );
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkDropPartitions(@NotNull MicroBenchmark bench,
                                                       @NotNull BenchData data,
                                                       int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      return bench.measure(
          () -> addManyPartitionsNoException(client, dbName, tableName, null,
              Collections.singletonList("d"), count),
          () -> throwingSupplierWrapper(() ->
              client.dropPartitions(dbName, tableName, null)),
          null
      );
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkAlterPartitions(@NotNull MicroBenchmark bench,
                                                        @NotNull BenchData data,
                                                        int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      return bench.measure(
          () -> addManyPartitionsNoException(client, dbName, tableName, null,
              Collections.singletonList("d"), count),
          () -> throwingSupplierWrapper(() -> {
            List<Partition> newPartitions = client.getPartitions(dbName, tableName);
            newPartitions.forEach(p -> {
              p.getParameters().put("new_param", "param_val");
              p.getSd().setCols(Arrays.asList(new FieldSchema("new_col", "string", null)));
            });
            client.alterPartitions(dbName, tableName, newPartitions);
            return null;
          }),
          () -> throwingSupplierWrapper(() ->
              client.dropPartitions(dbName, tableName, null))
      );
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkGetPartitionNames(@NotNull MicroBenchmark bench,
                                                          @NotNull BenchData data,
                                                          int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      addManyPartitionsNoException(client, dbName, tableName, null,
          Collections.singletonList("d"), count);
      return bench.measure(
          () -> throwingSupplierWrapper(() -> client.getPartitionNames(dbName, tableName))
      );
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkGetPartitionsByName(@NotNull MicroBenchmark bench,
                                                            @NotNull BenchData data,
                                                            int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      addManyPartitionsNoException(client, dbName, tableName, null,
          Collections.singletonList("d"), count);
      List<String> partitionNames = throwingSupplierWrapper(() ->
          client.getPartitionNames(dbName, tableName));
      return bench.measure(
          () ->
              throwingSupplierWrapper(() ->
                  client.getPartitionsByNames(dbName, tableName, partitionNames))
      );
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkGetPartitionsByFilter(@NotNull MicroBenchmark bench,
                                                              @NotNull BenchData data,
                                                              int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      addManyPartitionsNoException(client, dbName, tableName, null,
              Collections.singletonList("d"), count);
      return bench.measure(
          () ->
              throwingSupplierWrapper(() ->
                  client.getPartitionsByFilter(dbName, tableName, "`date`='d0'"))
      );
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkRenameTable(@NotNull MicroBenchmark bench,
                                                    @NotNull BenchData data,
                                                    int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      addManyPartitionsNoException(client, dbName, tableName, null,
          Collections.singletonList("d"), count);
      Table oldTable = client.getTable(dbName, tableName);
      oldTable.getSd().setLocation("");
      Table newTable = oldTable.deepCopy();
      newTable.setTableName(tableName + "_renamed");

      return bench.measure(
          () -> {
            // Measuring 2 renames, so the tests are idempotent
            throwingSupplierWrapper(() ->
                client.alterTable(oldTable.getDbName(), oldTable.getTableName(), newTable));
            throwingSupplierWrapper(() ->
                client.alterTable(newTable.getDbName(), newTable.getTableName(), oldTable));
          }
      );
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
  }

  static DescriptiveStatistics benchmarkDropDatabase(@NotNull MicroBenchmark bench,
                                                     @NotNull BenchData data,
                                                     int count) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;

    throwingSupplierWrapper(() -> client.dropDatabase(dbName));
    try {
      return bench.measure(
          () -> {
            throwingSupplierWrapper(() -> client.createDatabase(dbName));
            BenchmarkUtils.createManyTables(client, count, dbName, "tmp_table_%d");
          },
          () -> throwingSupplierWrapper(() -> client.dropDatabase(dbName)),
          null
      );
    } finally {
      throwingSupplierWrapper(() -> client.createDatabase(dbName));
    }
  }

  static DescriptiveStatistics benchmarkOpenTxns(@NotNull MicroBenchmark bench,
                                                 @NotNull BenchData data,
                                                 int howMany) {
    final HMSClient client = data.getClient();
    return bench.measure(null,
        () -> throwingSupplierWrapper(() -> client.openTxn(howMany)),
        () -> throwingSupplierWrapper(() -> client.abortTxns(client.getOpenTxns())));
  }

  static DescriptiveStatistics benchmarkAllocateTableWriteIds(@NotNull MicroBenchmark bench,
                                                              @NotNull BenchData data,
                                                              int howMany) {
    final HMSClient client = data.getClient();

    return  bench.measure(
        () -> throwingSupplierWrapper(() -> client.openTxn(howMany)),
        () -> throwingSupplierWrapper(() -> client.allocateTableWriteIds("test_db", "test_tbl", client.getOpenTxns())),
        () -> throwingSupplierWrapper(() -> client.abortTxns(client.getOpenTxns()))
    );
  }

  static DescriptiveStatistics benchmarkGetValidWriteIds(@NotNull MicroBenchmark bench,
                                                         @NotNull BenchData data,
                                                         int howMany) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    List<String> tableNames = new ArrayList<>();

    return bench.measure(
        () -> {
          BenchmarkUtils.createManyTables(client, howMany, dbName, "tmp_table_%d");
          for (int i = 0; i < howMany; i++) {
            tableNames.add(dbName + ".tmp_table_" + i);
          }
        },
        () -> throwingSupplierWrapper(() -> client.getValidWriteIds(tableNames)),
        () -> {
          BenchmarkUtils.dropManyTables(client, howMany, dbName, "tmp_table_%d");
        }
    );
  }

  static DescriptiveStatistics benchmarkGetNotificationId(@NotNull MicroBenchmark benchmark,
                                                          @NotNull BenchData data) {
    HMSClient client = data.getClient();
    return benchmark.measure(() ->
        throwingSupplierWrapper(client::getCurrentNotificationId));
  }

  static DescriptiveStatistics benchmarkPartitionManagement(@NotNull MicroBenchmark bench,
                                                            @NotNull BenchData data,
                                                            int tableCount) {

    String dbName = data.dbName + "_" + tableCount, tableNamePrefix = data.tableName;
    final HMSClient client = data.getClient();
    final PartitionManagementTask partitionManagementTask = new PartitionManagementTask();
    final List<Path> paths = new ArrayList<>();
    final FileSystem fs;
    try {
      fs = FileSystem.get(client.getHadoopConf());
      client.getHadoopConf().set("hive.metastore.uris", client.getServerURI().toString());
      client.getHadoopConf().set("metastore.partition.management.database.pattern", dbName);
      partitionManagementTask.setConf(client.getHadoopConf());

      client.createDatabase(dbName);
      for (int i = 0; i < tableCount; i++) {
        String tableName = tableNamePrefix + "_" + i;
        Util.TableBuilder tableBuilder = new Util.TableBuilder(dbName, tableName).withType(TableType.MANAGED_TABLE)
            .withColumns(createSchema(Arrays.asList(new String[] {"astring:string", "aint:int", "adouble:double", "abigint:bigint"})))
            .withPartitionKeys(createSchema(Collections.singletonList("d")));
        boolean enableDynamicPart = i % 5 == 0;
        if (enableDynamicPart) {
          tableBuilder.withParameter("discover.partitions", "true");
        }
        client.createTable(tableBuilder.build());
        addManyPartitionsNoException(client, dbName, tableName, null, Collections.singletonList("d"), 500);
        if (enableDynamicPart) {
          Table t = client.getTable(dbName, tableName);
          Path tabLoc = new Path(t.getSd().getLocation());
          for (int j = 501; j <= 1000; j++) {
            Path path = new Path(tabLoc, "d=d" + j + "_1");
            paths.add(path);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final AtomicLong id = new AtomicLong(0);
    ExecutorService service = Executors.newFixedThreadPool(20);
    Runnable preRun = () -> {
      int len = paths.size() / 20;
      id.getAndIncrement();
      List<Future> futures = new ArrayList<>();
      for (int i = 0; i <= 20; i++) {
        int k = i;
        futures.add(service.submit((Callable<Void>) () -> {
          for (int j = k * len; j < (k + 1) * len && j < paths.size(); j++) {
            Path path = paths.get(j);
            if (id.get() == 1) {
              fs.mkdirs(path);
            } else {
              String fileName = path.getName().split("_")[0];
              long seq = id.get();
              Path destPath = new Path(path.getParent(), fileName + "_" + seq);
              Path sourcePath = new Path(path.getParent(), fileName + "_" + (seq-1));
              fs.rename(sourcePath, destPath);
            }
          }
          return null;
        }));
      }
      for (Future future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          service.shutdown();
          throw new RuntimeException(e);
        }
      }
    };

    try {
      return bench.measure(preRun, partitionManagementTask, null);
    } finally {
      service.shutdown();
    }
  }

}
