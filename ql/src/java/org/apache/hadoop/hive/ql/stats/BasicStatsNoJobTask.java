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

package org.apache.hadoop.hive.ql.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.plan.BasicStatsNoJobWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * StatsNoJobTask is used in cases where stats collection is the only task for the given query (no
 * parent MR or Tez job). It is used in the following cases 1) ANALYZE with noscan for
 * file formats that implement StatsProvidingRecordReader interface: ORC format (implements
 * StatsProvidingRecordReader) stores column statistics for all columns in the file footer. It's much
 * faster to compute the table/partition statistics by reading the footer than scanning all the
 * rows. This task can be used for computing basic stats like numFiles, numRows, fileSize,
 * rawDataSize from ORC footer.
 * However, this cannot be used for full ACID tables, since some files may contain updates
 * and deletes to existing rows, so summing up the per-file row counts is invalid.
 **/
public class BasicStatsNoJobTask implements IStatsProcessor {

  private static transient final Logger LOG = LoggerFactory.getLogger(BasicStatsNoJobTask.class);
  private HiveConf conf;

  private BasicStatsNoJobWork work;
  private LogHelper console;

  public BasicStatsNoJobTask(HiveConf conf, BasicStatsNoJobWork work) {
    this.conf = conf;
    this.work = work;
    console = new LogHelper(LOG);
  }

  public static boolean canUseBasicStats(
      Table table, Class<? extends InputFormat> inputFormat) {
      return canUseFooterScan(table, inputFormat) || useBasicStatsFromStorageHandler(table);
  }

  public static boolean canUseFooterScan(Table table, Class<? extends InputFormat> inputFormat) {
    return (OrcInputFormat.class.isAssignableFrom(inputFormat) && !AcidUtils.isFullAcidTable(table))
        || MapredParquetInputFormat.class.isAssignableFrom(inputFormat);
  }

  private static boolean useBasicStatsFromStorageHandler(Table table) {
    return table.isNonNative() && table.getStorageHandler().canProvideBasicStatistics();
  }


  @Override
  public void initialize(CompilationOpContext opContext) {

  }

  @Override
  public int process(Hive db, Table tbl) throws Exception {

    LOG.info("Executing stats (no job) task");

    ExecutorService threadPool = StatsTask.newThreadPool(conf);

    return aggregateStats(threadPool, db);
  }

  public StageType getType() {
    return StageType.STATS;
  }

  public String getName() {
    return "STATS-NO-JOB";
  }

  abstract static class StatCollector implements Runnable {

    protected Partish partish;
    protected Object result;
    protected LogHelper console;

    public static Function<StatCollector, String> SIMPLE_NAME_FUNCTION =
        sc -> String.format("%s#%s", sc.partish.getTable().getCompleteName(), sc.partish.getPartishType());

    public static Function<StatCollector, Partition> EXTRACT_RESULT_FUNCTION = sc -> (Partition) sc.result;

    protected void init(HiveConf conf, LogHelper console) throws IOException {
      this.console = console;
    }

    protected final boolean isValid() {
      return result != null;
    }

    protected final String toString(Map<String, String> parameters) {
      return StatsSetupConst.SUPPORTED_STATS.stream().map(st -> st + "=" + parameters.get(st))
          .collect(Collectors.joining(", "));
    }
  }

  static class HiveStorageHandlerStatCollector extends StatCollector {

    public HiveStorageHandlerStatCollector(Partish partish) {
      this.partish = partish;
    }

    @Override
    public void run() {
      try {
        Table table = partish.getTable();
        Map<String, String> parameters;
        Map<String, String> basicStatistics = table.getStorageHandler().getBasicStatistics(partish);
        if (partish.getPartition() != null) {
          parameters = partish.getPartParameters();
          result = new Partition(table, partish.getPartition().getTPartition());
        } else {
          parameters = table.getParameters();
          result = new Table(table.getTTable());
        }
        parameters.putAll(basicStatistics);
        StatsSetupConst.setBasicStatsState(parameters, StatsSetupConst.TRUE);
        String msg = partish.getSimpleName() + " stats: [" + toString(parameters) + ']';
        LOG.debug(msg);
        console.printInfo(msg);
      } catch (Exception e) {
        console.printInfo("[Warning] could not update stats for " + partish.getSimpleName() + ".",
            "Failed with exception " + e.getMessage() + "\n" + StringUtils.stringifyException(e));
      }
    }
  }

  static class FooterStatCollector extends StatCollector {

    private JobConf jc;
    private Path dir;
    private FileSystem fs;
    private LogHelper console;

    public FooterStatCollector(JobConf jc, Partish partish) {
      this.jc = jc;
      this.partish = partish;
    }

    @Override
    public void init(HiveConf conf, LogHelper console) throws IOException {
      this.console = console;
      dir = new Path(partish.getPartSd().getLocation());
      fs = dir.getFileSystem(conf);
    }

    @Override
    public void run() {

      Map<String, String> parameters = partish.getPartParameters();
      try {
        long numRows = 0;
        long rawDataSize = 0;
        long fileSize = 0;
        long numFiles = 0;
        long numErasureCodedFiles = 0;
        // Note: this code would be invalid for transactional tables of any kind.
        Utilities.FILE_OP_LOGGER.debug("Aggregating stats for {}", dir);
        List<FileStatus> fileList = null;
        if (partish.getTable() != null
            && AcidUtils.isTransactionalTable(partish.getTable())) {
          fileList = AcidUtils.getAcidFilesForStats(partish.getTable(), dir, jc, fs);
        } else {
          fileList = HiveStatsUtils.getFileStatusRecurse(dir, -1, fs);
        }
        ThreadPoolExecutor tpE = null;
        List<Future<FileStats>> futures = null;
        int numThreadsFactor = HiveConf.getIntVar(jc, HiveConf.ConfVars.BASIC_STATS_TASKS_MAX_THREADS_FACTOR);
        if (fileList.size() > 1 && numThreadsFactor > 0) {
          int numThreads = Math.min(fileList.size(), numThreadsFactor * Runtime.getRuntime().availableProcessors());
          ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Basic-Stats-Thread-%d").build();
          tpE = new ThreadPoolExecutor(numThreads, numThreads, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
              threadFactory);
          tpE.allowsCoreThreadTimeOut();
          futures = new ArrayList<>();
          LOG.info("Processing Stats for {} file using {} threads", fileList.size(), numThreads);
        }

        for (FileStatus file : fileList) {
          Utilities.FILE_OP_LOGGER.debug("Computing stats for {}", file);
          if (!file.isDirectory()) {
            InputFormat<?, ?> inputFormat = ReflectionUtil.newInstance(partish.getInputFormatClass(), jc);
            InputSplit dummySplit = new FileSplit(file.getPath(), 0, -1, new String[] { partish.getLocation() });
            if (file.getLen() == 0) {
              numFiles += 1;
            } else {
              FileStatProcessor fsp = new FileStatProcessor(file, inputFormat, dummySplit, jc);
              if (tpE != null) {
                futures.add(tpE.submit(fsp));
              } else {
                // No parallel processing, just call the method normally & update the stats.
                FileStats fileStat = fsp.call();
                rawDataSize += fileStat.getRawDataSize();
                numRows += fileStat.getNumRows();
                fileSize += fileStat.getFileSize();
                numFiles += 1;
                numErasureCodedFiles += fileStat.getNumErasureCodedFiles();
              }
            }
          }
        }

        if (tpE != null) {
          try {
            for (Future<FileStats> future : futures) {
              FileStats fileStat = future.get();
              rawDataSize += fileStat.getRawDataSize();
              numRows += fileStat.getNumRows();
              fileSize += fileStat.getFileSize();
              numFiles += 1;
              numErasureCodedFiles += fileStat.getNumErasureCodedFiles();
            }
          } catch (Exception e) {
            LOG.error("Encountered exception while collecting stats for file lists as {}", fileList, e);
            // Cancel all the futures in the list & throw the caught exception post that.
            futures.forEach(x -> x.cancel(true));
            throw e;
          } finally {
            tpE.shutdown();
          }
        }
        StatsSetupConst.setBasicStatsState(parameters, StatsSetupConst.TRUE);

        parameters.put(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
        parameters.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(rawDataSize));
        parameters.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(fileSize));
        parameters.put(StatsSetupConst.NUM_FILES, String.valueOf(numFiles));
        parameters.put(StatsSetupConst.NUM_ERASURE_CODED_FILES, String.valueOf(numErasureCodedFiles));

        if (partish.getPartition() != null) {
          result = new Partition(partish.getTable(), partish.getPartition().getTPartition());
        } else {
          result = new Table(partish.getTable().getTTable());
        }

        String msg = partish.getSimpleName() + " stats: [" + toString(parameters) + ']';
        LOG.debug(msg);
        console.printInfo(msg);

      } catch (Exception e) {
        console.printInfo("[Warning] could not update stats for " + partish.getSimpleName() + ".", "Failed with exception " + e.getMessage() + "\n" + StringUtils.stringifyException(e));
      }
    }

  }

  private Collection<Partition> getPartitions(Table table) {
    Collection<Partition> partitions = null;
    if (work.getPartitions() == null || work.getPartitions().isEmpty()) {
      if (table.isPartitioned()) {
        partitions = table.getTableSpec().partitions;
      }
    } else {
      partitions = work.getPartitions();
    }
    return partitions;
  }

  private List<Partish> getPartishes(Table table) {
    Collection<Partition> partitions = getPartitions(table);
    List<Partish> partishes = Lists.newLinkedList();
    if (partitions == null) {
      partishes.add(Partish.buildFor(table));
    } else {
      for (Partition part : partitions) {
        partishes.add(Partish.buildFor(table, part));
      }
    }
    return partishes;
  }


  private int aggregateStats(ExecutorService threadPool, Hive db) {
    int ret = 0;
    try {
      JobConf jc = new JobConf(conf);

      TableSpec tableSpecs = work.getTableSpecs();

      if (tableSpecs == null) {
        throw new RuntimeException("this is unexpected...needs some investigation");
      }

      Table table = tableSpecs.tableHandle;

      List<Partish> partishes = getPartishes(table);

      List<StatCollector> scs = new ArrayList();
      for (Partish partish : partishes) {
        if (useBasicStatsFromStorageHandler(table)) {
          scs.add(new HiveStorageHandlerStatCollector(partish));
        } else {
          scs.add(new FooterStatCollector(jc, partish));
        }
      }

      for (StatCollector sc : scs) {
        sc.init(conf, console);
        threadPool.execute(sc);
      }

      LOG.debug("Stats collection waiting for threadpool to shutdown..");
      shutdownAndAwaitTermination(threadPool);
      LOG.debug("Stats collection threadpool shutdown successful.");

      ret = updatePartitions(db, scs, table);

    } catch (Exception e) {
      console.printError("Failed to collect footer statistics.", "Failed with exception " + e.getMessage() + "\n" + StringUtils.stringifyException(e));
      // Fail the query if the stats are supposed to be reliable
      if (work.isStatsReliable()) {
        ret = -1;
      }
    }

    // The return value of 0 indicates success,
    // anything else indicates failure
    return ret;
  }

  private int updatePartitions(Hive db, List<StatCollector> scs, Table table) throws InvalidOperationException, HiveException {

    String tableFullName = table.getFullyQualifiedName();

    if (scs.isEmpty()) {
      return 0;
    }
    if (work.isStatsReliable()) {
      for (StatCollector statsCollection : scs) {
        if (!statsCollection.isValid()) {
          LOG.debug("Stats requested to be reliable. Empty stats found: {}", statsCollection.partish.getSimpleName());
          return -1;
        }
      }
    }
    List<StatCollector> validColectors = Lists.newArrayList();
    for (StatCollector statsCollection : scs) {
      if (statsCollection.isValid()) {
        validColectors.add(statsCollection);
      }
    }

    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);

    ImmutableListMultimap<String, StatCollector> collectorsByTable = Multimaps.index(validColectors, FooterStatCollector.SIMPLE_NAME_FUNCTION);

    LOG.debug("Collectors.size(): {}", collectorsByTable.keySet());

    if (collectorsByTable.keySet().size() < 1) {
      LOG.warn("Collectors are empty! ; {}", tableFullName);
    }

    // for now this should be true...
    assert (collectorsByTable.keySet().size() <= 1);

    LOG.debug("Updating stats for: {}", tableFullName);

    for (String partName : collectorsByTable.keySet()) {
      ImmutableList<StatCollector> values = collectorsByTable.get(partName);

      if (values == null) {
        throw new RuntimeException("very intresting");
      }

      if (values.get(0).result instanceof Table) {
        db.alterTable(tableFullName, (Table) values.get(0).result, environmentContext, true);
        LOG.debug("Updated stats for {}.", tableFullName);
      } else {
        if (values.get(0).result instanceof Partition) {
          List<Partition> results = Lists.transform(values, StatCollector.EXTRACT_RESULT_FUNCTION);
          db.alterPartitions(tableFullName, results, environmentContext, true);
          LOG.debug("Bulk updated {} partitions of {}.", results.size(), tableFullName);
        } else {
          throw new RuntimeException("inconsistent");
        }
      }
    }
    LOG.debug("Updated stats for: {}", tableFullName);
    return 0;
  }

  private void shutdownAndAwaitTermination(ExecutorService threadPool) {

    // Disable new tasks from being submitted
    threadPool.shutdown();
    try {

      // Wait a while for existing tasks to terminate
      // XXX this will wait forever... :)
      while (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.debug("Waiting for all stats tasks to finish...");
      }
      // Cancel currently executing tasks
      threadPool.shutdownNow();

      // Wait a while for tasks to respond to being cancelled
      if (!threadPool.awaitTermination(100, TimeUnit.SECONDS)) {
        LOG.debug("Stats collection thread pool did not terminate");
      }
    } catch (InterruptedException ie) {

      // Cancel again if current thread also interrupted
      threadPool.shutdownNow();

      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void setDpPartSpecs(Collection<Partition> dpPartSpecs) {
  }

  /**
   * Utility class to process file level stats in parallel.
   */
  private static class FileStatProcessor implements Callable <FileStats> {

    private final InputSplit dummySplit;
    private final InputFormat<?, ?> inputFormat;
    private final JobConf jc;
    private final FileStatus file;

    FileStatProcessor(FileStatus file, InputFormat<?, ?> inputFormat, InputSplit dummySplit, JobConf jc) {
      this.file = file;
      this.dummySplit = dummySplit;
      this.inputFormat = inputFormat;
      this.jc = jc;
    }

    @Override
    public FileStats call() throws Exception {
      try (org.apache.hadoop.mapred.RecordReader<?, ?> recordReader = inputFormat
          .getRecordReader(dummySplit, jc, Reporter.NULL)) {
        if (recordReader instanceof StatsProvidingRecordReader) {
          StatsProvidingRecordReader statsRR;
          statsRR = (StatsProvidingRecordReader) recordReader;
          final FileStats fileStats =
              new FileStats(statsRR.getStats().getRawDataSize(), statsRR.getStats().getRowCount(), file.getLen(),
                  file.isErasureCoded());
          return fileStats;
        } else {
          throw new HiveException(String.format("Unexpected file found during reading footers for: %s ", file));
        }
      }
    }
  }

  /**
   * Utility class for holding the file level statistics.
   */
  private static class FileStats {

    private long numRows = 0;
    private long rawDataSize = 0;
    private long fileSize = 0;
    private long numErasureCodedFiles = 0;

    public FileStats(long rawDataSize, long numRows, long fileSize, boolean isErasureCoded) {
      this.rawDataSize = rawDataSize;
      this.numRows = numRows;
      this.fileSize = fileSize;
      this.numErasureCodedFiles = isErasureCoded ? 1 : 0;
    }

    public long getNumRows() {
      return numRows;
    }

    public long getRawDataSize() {
      return rawDataSize;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getNumErasureCodedFiles() {
      return numErasureCodedFiles;
    }

  }
}
