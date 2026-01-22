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
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.metastore.tools.Util.addManyPartitions;
import static org.apache.hadoop.hive.metastore.tools.Util.addManyPartitionsNoException;
import static org.apache.hadoop.hive.metastore.tools.Util.createManyPartitions;
import static org.apache.hadoop.hive.metastore.tools.Util.createSchema;
import static org.apache.hadoop.hive.metastore.tools.Util.throwingSupplierWrapper;
import static org.apache.hadoop.hive.metastore.tools.Util.updateManyPartitionsStatsNoException;

/**
 * Actual benchmark code.
 */
final class HMSBenchmarks {
  private static final Logger LOG = LoggerFactory.getLogger(HMSBenchmarks.class);

  private static final String PARAM_KEY = "parameter_";
  private static final String PARAM_VALUE = "value_";

  static Map<String, DescriptiveStatistics> benchmarkListDatabases(@NotNull MicroBenchmark benchmark,
                                                      @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    Map<String, DescriptiveStatistics> res = new HashMap<>();
    res.put("", benchmark.measure(() ->
        throwingSupplierWrapper(() -> client.getAllDatabases(null))));
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkTableCreate(@NotNull MicroBenchmark bench,
                                                    @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Table table = Util.TableBuilder.buildDefaultTable(dbName, tableName);
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    res.put("", bench.measure(null,
        () -> throwingSupplierWrapper(() -> client.createTable(table)),
        () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName))));
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkDeleteCreate(@NotNull MicroBenchmark bench,
                                                     @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Table table = Util.TableBuilder.buildDefaultTable(dbName, tableName);
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    res.put("", bench.measure(
        () -> throwingSupplierWrapper(() -> client.createTable(table)),
        () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName)),
        null));
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkDeleteWithPartitions(@NotNull MicroBenchmark bench,
                                                             @NotNull BenchData data,
                                                             int[] partitionCounts,
                                                             int nparams) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    // Create many parameters
    Map<String, String> parameters = new HashMap<>(nparams);
    for (int i = 0; i < nparams; i++) {
      parameters.put(PARAM_KEY + i, PARAM_VALUE + i);
    }

    for (int count : partitionCounts) {
      res.put("." + count, bench.measure(
          () -> throwingSupplierWrapper(() -> {
            BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
            addManyPartitions(client, dbName, tableName, parameters,
                Collections.singletonList("d"), count);
            return true;
          }),
          () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName)),
          null));
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkDeleteMetaOnlyWithPartitions(@NotNull MicroBenchmark bench,
                                                                        @NotNull BenchData data,
                                                                        int[] partitionCounts,
                                                                        int nparams) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    // Create many parameters
    Map<String, String> parameters = new HashMap<>(nparams);
    for (int i = 0; i < nparams; i++) {
      parameters.put(PARAM_KEY + i, PARAM_VALUE + i);
    }

    for (int count : partitionCounts) {
      res.put("." + count, bench.measure(
            () -> throwingSupplierWrapper(() -> {
              BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
              addManyPartitions(client, dbName, tableName, parameters,
                      Collections.singletonList("d"), count);
              return true;
            }),
            () -> throwingSupplierWrapper(() -> client.dropTable(dbName, tableName, false)),
            null));
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkGetTable(@NotNull MicroBenchmark bench,
                                                 @NotNull BenchData data) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
    try {
      res.put("", bench.measure(() ->
          throwingSupplierWrapper(() -> client.getTable(dbName, tableName))));
    } finally {
      throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkListTables(@NotNull MicroBenchmark bench,
                                                   @NotNull BenchData data,
                                                   int[] tableCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    // Create a bunch of tables
    String format = "tmp_table_%d";
    for (int count : tableCounts) {
      try {
        BenchmarkUtils.createManyTables(client, count, dbName, format);
        res.put("." + count, bench.measure(() ->
            throwingSupplierWrapper(() -> client.getAllTables(dbName, null))));
      } finally {
        BenchmarkUtils.dropManyTables(client, count, dbName, format);
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkCreatePartition(@NotNull MicroBenchmark bench,
                                                        @NotNull BenchData data,
                                                        int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        Table t = throwingSupplierWrapper(() -> client.getTable(dbName, tableName));
        List<Partition> parts = createManyPartitions(t, null, Collections.singletonList("d"), count);

        res.put("." + count, bench.measure(null,
            () -> throwingSupplierWrapper(() -> {
              parts.forEach(part -> throwingSupplierWrapper(() -> client.addPartition(part)));
              return null;
            }),
            () -> throwingSupplierWrapper(() -> client.dropPartitions(dbName, tableName, null))));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkListPartitions(@NotNull MicroBenchmark bench,
                                                           @NotNull BenchData data,
                                                           int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        addManyPartitionsNoException(client, dbName, tableName, null, Collections.singletonList("d"), count);
        LOG.debug("Created {} partitions", count);
        LOG.debug("started benchmark... ");
        res.put("." + count, bench.measure(() ->
            throwingSupplierWrapper(() -> client.listPartitions(dbName, tableName))));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkGetPartitions(@NotNull MicroBenchmark bench,
                                                      @NotNull BenchData data,
                                                      int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        addManyPartitionsNoException(client, dbName, tableName, null, Collections.singletonList("d"), count);
        LOG.debug("Created {} partitions", count);
        LOG.debug("started benchmark... ");
        res.put("." + count, bench.measure(() ->
            throwingSupplierWrapper(() -> client.getPartitions(dbName, tableName))));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkDropPartition(@NotNull MicroBenchmark bench,
                                                      @NotNull BenchData data,
                                                      int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        res.put("." + count, bench.measure(
            () -> addManyPartitionsNoException(client, dbName, tableName, null,
                    Collections.singletonList("d"), count),
            () -> throwingSupplierWrapper(() -> {
              List<String> partNames = client.getPartitionNames(dbName, tableName);
              partNames.forEach(partName ->
                  throwingSupplierWrapper(() -> client.dropPartition(dbName, tableName, partName)));
              return null;
            }),
            null));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkCreatePartitions(@NotNull MicroBenchmark bench,
                                                         @NotNull BenchData data,
                                                         int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        res.put("." + count, bench.measure(
            null,
            () -> addManyPartitionsNoException(client, dbName, tableName, null,
                Collections.singletonList("d"), count),
            () -> throwingSupplierWrapper(() ->
                client.dropPartitions(dbName, tableName, null))
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkDropPartitions(@NotNull MicroBenchmark bench,
                                                       @NotNull BenchData data,
                                                       int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        res.put("." + count, bench.measure(
            () -> addManyPartitionsNoException(client, dbName, tableName, null,
                Collections.singletonList("d"), count),
            () -> throwingSupplierWrapper(() ->
                client.dropPartitions(dbName, tableName, null)),
            null
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkAlterPartitions(@NotNull MicroBenchmark bench,
                                                        @NotNull BenchData data,
                                                        int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        res.put("." + count, bench.measure(
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
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkGetPartitionNames(@NotNull MicroBenchmark bench,
                                                          @NotNull BenchData data,
                                                          int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        addManyPartitionsNoException(client, dbName, tableName, null,
            Collections.singletonList("d"), count);
        res.put("." + count, bench.measure(
            () -> throwingSupplierWrapper(() -> client.getPartitionNames(dbName, tableName))
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkGetPartitionsByName(@NotNull MicroBenchmark bench,
                                                            @NotNull BenchData data,
                                                            int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        addManyPartitionsNoException(client, dbName, tableName, null,
            Collections.singletonList("d"), count);
        List<String> partitionNames = throwingSupplierWrapper(() ->
            client.getPartitionNames(dbName, tableName));
        res.put("." + count, bench.measure(
            () ->
                throwingSupplierWrapper(() ->
                    client.getPartitionsByNames(dbName, tableName, partitionNames))
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkGetPartitionsByFilter(@NotNull MicroBenchmark bench,
                                                              @NotNull BenchData data,
                                                              int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName, createSchema(Arrays.asList("p_a", "p_b", "p_c")));
      try {
        // Create multiple partitions with values: [a0, b0, c0], [a0, b1, c1], [a0, b2, c2]...
        List<List<String>> values = IntStream.range(0, count)
            .mapToObj(i -> Arrays.asList("a0", "b" + i, "c" + i))
            .collect(Collectors.toList());
        addManyPartitionsNoException(client, dbName, tableName, null, values);
        res.put("#simple." + count, bench.measure(
            () ->
                throwingSupplierWrapper(() -> {
                    client.getPartitionsByFilter(dbName, tableName, "`p_b`='b0'");
                return null;
                })
        ));
        res.put("#multiOr." + count, bench.measure(
            () ->
                throwingSupplierWrapper(() -> {
                    client.getPartitionsByFilter(dbName, tableName,
                        " `p_b`='b0' or `p_c`='c0' or `p_b`='b1' or `p_c`='c1'");
                return null;
                })
        ));
        res.put("#multiAnd." + count, bench.measure(
            () ->
                throwingSupplierWrapper(() -> {
                    client.getPartitionsByFilter(dbName, tableName, "`p_a`='a0' and `p_b`='b0' and `p_c`='c0'");
                return null;
                })
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkGetPartitionsByPs(@NotNull MicroBenchmark bench,
                                                          @NotNull BenchData data,
                                                          int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName, createSchema(Arrays.asList("p_a", "p_b", "p_c")));
      try {
        // Create multiple partitions with values: [a0, b0, c0], [a0, b1, c1], [a0, b2, c2]...
        List<List<String>> values = IntStream.range(0, count)
            .mapToObj(i -> Arrays.asList("a0", "b" + i, "c" + i))
            .collect(Collectors.toList());
        addManyPartitionsNoException(client, dbName, tableName, null, values);
        res.put("." + count, bench.measure(
            () ->
                throwingSupplierWrapper(() ->
                    client.getPartitionsByPs(dbName, tableName, Arrays.asList("a0")))
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }


  static Map<String, DescriptiveStatistics> benchmarkGetPartitionsStat(@NotNull MicroBenchmark bench,
                                                          @NotNull BenchData data,
                                                          int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        addManyPartitionsNoException(client, dbName, tableName, null,
                Collections.singletonList("d"), count);
        List<String> partNames = throwingSupplierWrapper(() ->
                client.getPartitionNames(dbName, tableName));
        updateManyPartitionsStatsNoException(client, dbName, tableName, partNames);
        PartitionsStatsRequest request = new PartitionsStatsRequest(
                dbName, tableName, Arrays.asList("name"), partNames);
        res.put("." + count, bench.measure(
            () ->
                throwingSupplierWrapper(() -> client.getPartitionsStats(request))
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkUpdatePartitionsStat(@NotNull MicroBenchmark bench,
                                                             @NotNull BenchData data,
                                                             int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        addManyPartitionsNoException(client, dbName, tableName, null,
                Collections.singletonList("d"), count);
        List<String> partNames = throwingSupplierWrapper(() ->
                client.getPartitionNames(dbName, tableName));
        res.put("." + count, bench.measure(
                () -> updateManyPartitionsStatsNoException(client, dbName, tableName, partNames)
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkRenameTable(@NotNull MicroBenchmark bench,
                                                    @NotNull BenchData data,
                                                    int[] partitionCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    String tableName = data.tableName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : partitionCounts) {
      BenchmarkUtils.createPartitionedTable(client, dbName, tableName);
      try {
        addManyPartitionsNoException(client, dbName, tableName, null,
            Collections.singletonList("d"), count);
        Table oldTable = throwingSupplierWrapper(() -> client.getTable(dbName, tableName));
        oldTable.getSd().setLocation("");
        Table newTable = oldTable.deepCopy();
        newTable.setTableName(tableName + "_renamed");

        res.put("." + count, bench.measure(
            () -> {
              // Measuring 2 renames, so the tests are idempotent
              throwingSupplierWrapper(() ->
                  client.alterTable(oldTable.getDbName(), oldTable.getTableName(), newTable));
              throwingSupplierWrapper(() ->
                  client.alterTable(newTable.getDbName(), newTable.getTableName(), oldTable));
            }
        ));
      } finally {
        throwingSupplierWrapper(() -> client.dropTable(dbName, tableName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkDropDatabase(@NotNull MicroBenchmark bench,
                                                     @NotNull BenchData data,
                                                     int[] tableCounts) {
    final HMSClient client = data.getClient();
    String dbName = data.dbName;
    Map<String, DescriptiveStatistics> res = new HashMap<>();

    for (int count : tableCounts) {
      throwingSupplierWrapper(() -> client.dropDatabase(dbName));
      try {
        res.put("." + count, bench.measure(
            () -> {
              throwingSupplierWrapper(() -> client.createDatabase(dbName));
              BenchmarkUtils.createManyTables(client, count, dbName, "tmp_table_%d");
            },
            () -> throwingSupplierWrapper(() -> client.dropDatabase(dbName)),
            null
        ));
      } finally {
        throwingSupplierWrapper(() -> client.createDatabase(dbName));
      }
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkOpenTxns(@NotNull MicroBenchmark bench,
                                                 @NotNull BenchData data,
                                                 int[] txnCounts) {
    final HMSClient client = data.getClient();
    Map<String, DescriptiveStatistics> res = new HashMap<>();
    for (int count : txnCounts) {
      res.put("." + count, bench.measure(null,
          () -> throwingSupplierWrapper(() -> client.openTxn(count)),
          () -> throwingSupplierWrapper(() -> client.abortTxns(client.getOpenTxns()))));
    }
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkGetNotificationId(@NotNull MicroBenchmark benchmark,
                                                          @NotNull BenchData data) {
    HMSClient client = data.getClient();
    Map<String, DescriptiveStatistics> res = new HashMap<>();
    res.put("", benchmark.measure(() ->
        throwingSupplierWrapper(client::getCurrentNotificationId)));
    return res;
  }

  static Map<String, DescriptiveStatistics> benchmarkPartitionManagement(@NotNull MicroBenchmark bench,
                                                            @NotNull BenchData data,
                                                            int[] tableCounts) {
    Map<String, DescriptiveStatistics> res = new HashMap<>();
    for (int tableCount : tableCounts) {
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
        res.put("." + tableCount, bench.measure(preRun, partitionManagementTask, null));
      } finally {
        service.shutdown();
      }
    }
    return res;
  }
}
