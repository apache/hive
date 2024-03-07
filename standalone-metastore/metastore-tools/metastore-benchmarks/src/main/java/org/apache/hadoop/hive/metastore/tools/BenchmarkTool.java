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
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.metastore.tools.Constants.HMS_DEFAULT_PORT;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkAlterPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkCreatePartition;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkCreatePartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDeleteCreate;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDeleteMetaOnlyWithPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDeleteWithPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDropDatabase;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDropPartition;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDropPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetNotificationId;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetPartitionNames;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetPartitionsByFilter;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetPartitionsByName;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetTable;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListAllTables;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListDatabases;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListManyPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListPartition;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListTables;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkOpenTxns;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkPartitionManagement;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkRenameTable;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkTableCreate;
import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/**
 * Command-line access to Hive Metastore.
 */
@SuppressWarnings( {"squid:S106", "squid:S1148"}) // Using System.out
@Command(name = "BenchmarkTool",
    mixinStandardHelpOptions = true, version = "1.0",
    showDefaultValues = true)

public class BenchmarkTool implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTool.class);
  private static final TimeUnit scale = TimeUnit.MILLISECONDS;
  private static final String CSV_SEPARATOR = "\t";
  private static final String TEST_TABLE = "bench_table";
  private enum RunModes {
    ACID,
    NONACID,
    ALL,
    MSCK // test PartitionManagementTask
  }


  @Option(names = {"-H", "--host"}, description = "HMS Host", paramLabel = "URI")
  private String host;

  @Option(names = {"-P", "--port"}, description = "HMS Server port")
  private Integer port = HMS_DEFAULT_PORT;

  @Option(names = {"-d", "--db"}, description = "database name")
  private String dbName = "bench_" + System.getProperty("user.name");

  @Option(names = {"-t", "--table"}, description = "table name")
  private String tableName = TEST_TABLE + "_" + System.getProperty("user.name");

  @Option(names = {"-N", "--number"}, description = "number of object instances")
  private int[] instances = {100};

  @Option(names = {"-L", "--spin"}, description = "spin count")
  private int spinCount = 100;

  @Option(names = {"-W", "--warmup"}, description = "warmup count")
  private int warmup = 15;

  @Option(names = {"-l", "--list"}, description = "list matching benchmarks")
  private boolean doList = false;

  @Option(names = {"-o", "--output"}, description = "output file")
  private String outputFile;

  @Option(names = {"-T", "--threads"}, description = "number of concurrent threads/clients")
  private int nThreads = 2;

  @Option(names = {"--confdir"}, description = "configuration directory")
  private String confDir;

  @Option(names = {"--sanitize"}, description = "sanitize results (remove outliers)")
  private boolean doSanitize = false;

  @Option(names = {"-C", "--csv"}, description = "produce CSV output")
  private boolean doCSV = false;

  @Option(names = {"--params"}, description = "number of table/partition parameters")
  private int[] nParameters = {0};

  @Option(names = {"--savedata"}, description = "save raw data in specified dir")
  private String dataSaveDir;

  @Option(names = {"--separator"}, description = "CSV field separator")
  private String csvSeparator = CSV_SEPARATOR;

  @Option(names = {"-M", "--pattern"}, description = "test name patterns")
  private Pattern[] matches;

  @Option(names = {"-E", "--exclude"}, description = "test name patterns to exclude")
  private Pattern[] exclude;

  @Option(names = {"--runMode"},
      description = "flag for setting the mode for the benchmark, acceptable values are: ACID, NONACID, ALL, MSCK")
  private RunModes runMode = RunModes.ALL;

  public static void main(String[] args) {
    CommandLine.run(new BenchmarkTool(), args);
  }

  static void saveData(Map<String,
      DescriptiveStatistics> result, String location, TimeUnit scale) throws IOException {
    Path dir = Paths.get(location);
    if (!dir.toFile().exists()) {
      LOG.debug("creating directory {}", location);
      Files.createDirectories(dir);
    } else if (!dir.toFile().isDirectory()) {
      LOG.error("{} should be a directory", location);
    }

    // Create a new file for each benchmark and dump raw data to it.
    result.forEach((name, data) -> saveDataFile(location, name, data, scale));
  }

  private static void saveDataFile(String location, String name,
                                   DescriptiveStatistics data, TimeUnit scale) {
    long conv = scale.toNanos(1);
    Path dst = Paths.get(location, name);
    try (PrintStream output = new PrintStream(dst.toString())) {
      // Print all values one per line
      Arrays.stream(data.getValues()).forEach(d -> output.println(d / conv));
    } catch (FileNotFoundException e) {
      LOG.error("failed to write to {}", dst);
    }
  }

  @Override
  public void run() {
    LOG.info("Using warmup " + warmup + " spin " + spinCount + " nparams " + Arrays.toString(nParameters) + " threads "
        + nThreads);
    HMSConfig.getInstance().init(host, port, confDir);

    preRunMsck(runMode == RunModes.MSCK);
    switch (runMode) {
      case ACID:
        runAcidBenchmarks();
        break;
      case MSCK:
      case NONACID:
        runNonAcidBenchmarks();
        break;
      case ALL:
      default:
        runNonAcidBenchmarks();
        runAcidBenchmarks();
        break;
    }
  }

  private void preRunMsck(boolean isMsck) {
    if (isMsck) {
      matches = new Pattern[]{Pattern.compile("PartitionManagementTask.*")};
    } else {
      List<Pattern> excludes = new ArrayList<>();
      Optional.ofNullable(exclude)
          .ifPresent(patterns -> Arrays.stream(patterns).forEach(p -> excludes.add(p)));
      excludes.add(Pattern.compile("PartitionManagementTask.*"));
      exclude = excludes.toArray(new Pattern[0]);
    }
  }

  private void runAcidBenchmarks() {
    ChainedOptionsBuilder optsBuilder =
        new OptionsBuilder()
            .warmupIterations(warmup)
            .measurementIterations(spinCount)
            .operationsPerInvocation(1)
            .mode(Mode.SingleShotTime)
            .timeUnit(TimeUnit.MILLISECONDS)
            .forks(0)
            .threads(nThreads)
            .syncIterations(true);

    String[] candidates = new String[] {
        ACIDBenchmarks.TestOpenTxn.class.getSimpleName(),
        ACIDBenchmarks.TestLocking.class.getSimpleName(),
        ACIDBenchmarks.TestGetValidWriteIds.class.getSimpleName(),
        ACIDBenchmarks.TestAllocateTableWriteIds.class.getSimpleName()
    };

    for (String pattern : Util.filterMatches(Arrays.asList(candidates), matches, exclude)) {
      optsBuilder = optsBuilder.include(pattern);
    }

    Options opts =
        optsBuilder
            .param("howMany", Arrays.stream(instances)
            .mapToObj(String::valueOf)
                .toArray(String[]::new))
            .param("nPartitions", Arrays.stream(nParameters)
                .mapToObj(String::valueOf).toArray(String[]::new))
            .build();

    try {
      new Runner(opts).run();
    } catch (RunnerException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private void runNonAcidBenchmarks() {
    StringBuilder sb = new StringBuilder();
    BenchData bData = new BenchData(dbName, tableName);

    MicroBenchmark bench = new MicroBenchmark(warmup, spinCount);
    BenchmarkSuite suite = new BenchmarkSuite();

    suite
        .setScale(scale)
        .doSanitize(doSanitize)
        .add("getNid", () -> benchmarkGetNotificationId(bench, bData))
        .add("listDatabases", () -> benchmarkListDatabases(bench, bData))
        .add("listTables", () -> benchmarkListAllTables(bench, bData))
        .add("getTable", () -> benchmarkGetTable(bench, bData))
        .add("createTable", () -> benchmarkTableCreate(bench, bData))
        .add("dropTable", () -> benchmarkDeleteCreate(bench, bData))
        .add("dropTableWithPartitions",
            () -> benchmarkDeleteWithPartitions(bench, bData, 1, nParameters[0]))
        .add("dropTableMetadataWithPartitions",
            () -> benchmarkDeleteMetaOnlyWithPartitions(bench, bData, 1, nParameters[0]))
        .add("addPartition", () -> benchmarkCreatePartition(bench, bData, 1))
        .add("dropPartition", () -> benchmarkDropPartition(bench, bData, 1))
        .add("listPartition", () -> benchmarkListPartition(bench, bData))
        .add("getPartition",
            () -> benchmarkGetPartitions(bench, bData, 1))
        .add("getPartitionNames",
            () -> benchmarkGetPartitionNames(bench, bData, 1))
        .add("getPartitionsByNames",
            () -> benchmarkGetPartitionsByName(bench, bData, 1))
        .add("getPartitionsByFilter",
            () -> benchmarkGetPartitionsByFilter(bench, bData, 1))
        .add("renameTable",
            () -> benchmarkRenameTable(bench, bData, 1))
        .add("dropDatabase",
            () -> benchmarkDropDatabase(bench, bData, 1))
        .add("openTxn",
            () -> benchmarkOpenTxns(bench, bData, 1))
        .add("PartitionManagementTask",
            () -> benchmarkPartitionManagement(bench, bData, 1));

    for (int howMany: instances) {
      suite.add("listTables" + '.' + howMany,
          () -> benchmarkListTables(bench, bData, howMany))
          .add("dropTableWithPartitions" + '.' + howMany,
              () -> benchmarkDeleteWithPartitions(bench, bData, howMany, nParameters[0]))
          .add("dropTableMetaOnlyWithPartitions" + '.' + howMany,
              () -> benchmarkDeleteMetaOnlyWithPartitions(bench, bData, howMany, nParameters[0]))
          .add("listPartitions" + '.' + howMany,
              () -> benchmarkListManyPartitions(bench, bData, howMany))
          .add("getPartitions" + '.' + howMany,
              () -> benchmarkGetPartitions(bench, bData, howMany))
          .add("getPartitionNames" + '.' + howMany,
              () -> benchmarkGetPartitionNames(bench, bData, howMany))
          .add("getPartitionsByNames" + '.' + howMany,
              () -> benchmarkGetPartitionsByName(bench, bData, howMany))
          .add("getPartitionsByFilter" + '.' + howMany,
              () -> benchmarkGetPartitionsByFilter(bench, bData, howMany))
          .add("addPartitions" + '.' + howMany,
              () -> benchmarkCreatePartitions(bench, bData, howMany))
          .add("dropPartitions" + '.' + howMany,
              () -> benchmarkDropPartitions(bench, bData, howMany))
          .add("alterPartitions" + '.' + howMany,
              () -> benchmarkAlterPartitions(bench, bData, howMany))
          .add("renameTable" + '.' + howMany,
              () -> benchmarkRenameTable(bench, bData, howMany))
          .add("dropDatabase" + '.' + howMany,
              () -> benchmarkDropDatabase(bench, bData, howMany))
          .add("addPartition" + '.' + howMany,
              () -> benchmarkCreatePartition(bench, bData, howMany))
          .add("dropPartition" + '.' + howMany,
              () -> benchmarkDropPartition(bench, bData, howMany))
          .add("openTxns" + '.' + howMany,
              () -> benchmarkOpenTxns(bench, bData, howMany))
          .add("PartitionManagementTask" + "." + howMany,
              () -> benchmarkPartitionManagement(bench, bData, howMany));
    }

    List<String> toRun = suite.listMatching(matches, exclude);
    if (toRun.isEmpty()) {
      return;
    }
    if (doList) {
      toRun.forEach(System.out::println);
      return;
    }

    LOG.info("Using table '{}.{}", dbName, tableName);

    try (HMSClient client = new HMSClient(getServerUri(host, String.valueOf(port)), confDir)) {
      bData.setClient(client);
      if (!client.dbExists(dbName)) {
        client.createDatabase(dbName);
      }

      if (client.tableExists(dbName, tableName)) {
        client.dropTable(dbName, tableName);
      }

      // Arrange various benchmarks in a suite
      BenchmarkSuite result = suite.runMatching(matches, exclude);

      Formatter fmt = new Formatter(sb);
      if (doCSV) {
        result.displayCSV(fmt, csvSeparator);
      } else {
        result.display(fmt);
      }

      PrintStream output = System.out;
      if (outputFile != null) {
        output = new PrintStream(outputFile);
      }

      if (outputFile != null) {
        // Print results to stdout as well
        StringBuilder s = new StringBuilder();
        Formatter f = new Formatter(s);
        result.display(f);
        System.out.print(s);
        f.close();
      }

      output.print(sb.toString());
      fmt.close();
      output.close();

      if (dataSaveDir != null) {
        saveData(result.getResult(), dataSaveDir, scale);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }
}
