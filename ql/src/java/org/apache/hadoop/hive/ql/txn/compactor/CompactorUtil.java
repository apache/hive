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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public class CompactorUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CompactorUtil.class);
  public static final String COMPACTOR = "compactor";
  /**
   * List of accepted properties for defining the compactor's job queue.
   *
   * The order is important and defines which property has precedence over the other if multiple properties are defined
   * at the same time.
   */
  private static final List<String> QUEUE_PROPERTIES = Arrays.asList(
      "compactor." + HiveConf.ConfVars.COMPACTOR_JOB_QUEUE.varname,
      "compactor.mapreduce.job.queuename",
      "compactor.mapred.job.queue.name"
  );

  public interface ThrowingRunnable<E extends Exception> {
    void run() throws E;

    static Runnable unchecked(ThrowingRunnable<?> r) {
      return () -> {
        try {
          r.run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    }
  }

  public static ExecutorService createExecutorWithThreadFactory(int parallelism, String threadNameFormat) {
    return new ForkJoinPool(parallelism,
      pool -> {
        ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName(format(threadNameFormat, worker.getPoolIndex()));
        return worker;
      },
      null, false);
  }

  /**
   * Get the compactor queue name if it's defined.
   * @param conf global hive conf
   * @param ci compaction info object
   * @param table instance of table
   * @return name of the queue
   */
  static String getCompactorJobQueueName(HiveConf conf, CompactionInfo ci, Table table) {
    // Get queue name from the ci. This is passed through
    // ALTER TABLE table_name COMPACT 'major' WITH OVERWRITE TBLPROPERTIES('compactor.hive.compactor.job.queue'='some_queue')
    List<Function<String, String>> propertyGetters = new ArrayList<>(2);
    if (ci.properties != null) {
      StringableMap ciProperties = new StringableMap(ci.properties);
      propertyGetters.add(ciProperties::get);
    }
    if (table.getParameters() != null) {
      propertyGetters.add(table.getParameters()::get);
    }

    for (Function<String, String> getter : propertyGetters) {
      for (String p : QUEUE_PROPERTIES) {
        String queueName = getter.apply(p);
        if (queueName != null && !queueName.isEmpty()) {
          return queueName;
        }
      }
    }
    return conf.getVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE);
  }

  public static StorageDescriptor resolveStorageDescriptor(Table t, Partition p) {
    return (p == null) ? t.getSd() : p.getSd();
  }

  public static StorageDescriptor resolveStorageDescriptor(Table t) {
    return resolveStorageDescriptor(t, null);
  }

  public static boolean isDynPartAbort(Table t, String partName) {
    return Optional.ofNullable(t).map(Table::getPartitionKeys).filter(pk -> !pk.isEmpty()).isPresent()
            && partName == null;
  }

  public static List<Partition> getPartitionsByNames(HiveConf conf, String dbName, String tableName, String partName) throws MetaException {
    try {
      return getMSForConf(conf).getPartitionsByNames(getDefaultCatalog(conf), dbName, tableName,
              Collections.singletonList(partName));
    } catch (Exception e) {
      LOG.error("Unable to get partitions by name = {}.{}.{}", dbName, tableName, partName);
      throw new MetaException(e.toString());
    }
  }

  public static Database resolveDatabase(HiveConf conf, String dbName) throws MetaException, NoSuchObjectException {
    try {
      return getMSForConf(conf).getDatabase(MetaStoreUtils.getDefaultCatalog(conf), dbName);
    } catch (NoSuchObjectException e) {
      LOG.error("Unable to find database {}, {}", dbName, e.getMessage());
      throw e;
    }
  }

  public static Table resolveTable(HiveConf conf, String dbName, String tableName) throws MetaException {
    try {
      return getMSForConf(conf).getTable(MetaStoreUtils.getDefaultCatalog(conf), dbName, tableName);
    } catch (MetaException e) {
      LOG.error("Unable to find table {}.{}, {}", dbName, tableName, e.getMessage());
      throw e;
    }
  }

  public static String getDebugInfo(List<Path> paths) {
    return "[" + paths.stream().map(Path::getName).collect(Collectors.joining(",")) + ']';
  }

  /**
   * Determine whether to run this job as the current user or whether we need a doAs to switch
   * users.
   * @param owner of the directory we will be working in, as determined by
   * {@link org.apache.hadoop.hive.metastore.txn.TxnUtils#findUserToRunAs(String, Table, Configuration)}
   * @return true if the job should run as the current user, false if a doAs is needed.
   */
  public static boolean runJobAsSelf(String owner) {
    return (owner.equals(System.getProperty("user.name")));
  }

  public static List<Path> getObsoleteDirs(AcidDirectory dir, boolean isDynPartAbort) {
    List<Path> obsoleteDirs = dir.getObsolete();
    /*
     * add anything in 'dir'  that only has data from aborted transactions - no one should be
     * trying to read anything in that dir (except getAcidState() that only reads the name of
     * this dir itself)
     * So this may run ahead of {@link CompactionInfo#highestWriteId} but it's ok (suppose there
     * are no active txns when cleaner runs).  The key is to not delete metadata about aborted
     * txns with write IDs > {@link CompactionInfo#highestWriteId}.
     * See {@link TxnStore#markCleaned(CompactionInfo)}
     */
    obsoleteDirs.addAll(dir.getAbortedDirectories());
    if (isDynPartAbort) {
      // In the event of an aborted DP operation, we should only consider the aborted directories for cleanup.
      // Including obsolete directories for partitioned tables can result in data loss.
      obsoleteDirs = dir.getAbortedDirectories();
    }
    return obsoleteDirs;
  }

  public static Partition resolvePartition(HiveConf conf, IMetaStoreClient msc, String dbName, String tableName, 
      String partName, METADATA_FETCH_MODE fetchMode) throws MetaException {
    if (partName != null) {
      List<Partition> parts = null;
      try {

        switch (fetchMode) {
          case LOCAL: parts = CompactorUtil.getPartitionsByNames(conf, dbName, tableName, partName);
                      break;
          case REMOTE: parts = RemoteCompactorUtil.getPartitionsByNames(msc, dbName, tableName, partName);
            break;
        }

        if (parts == null || parts.size() == 0) {
          // The partition got dropped before we went looking for it.
          return null;
        }
      } catch (Exception e) {
        LOG.error("Unable to find partition " + getFullPartitionName(dbName, tableName, partName), e);
        throw e;
      }
      if (parts.size() != 1) {
        LOG.error(getFullPartitionName(dbName, tableName, partName) + " does not refer to a single partition. " +
            Arrays.toString(parts.toArray()));
        throw new MetaException("Too many partitions for : " + getFullPartitionName(dbName, tableName, partName));
      }
      return parts.get(0);
    } else {
      return null;
    }
  }

  public static String getFullPartitionName(String dbName, String tableName, String partName) {
    StringBuilder buf = new StringBuilder();
    buf.append(dbName);
    buf.append('.');
    buf.append(tableName);
    if (partName != null) {
      buf.append('.');
      buf.append(partName);
    }
    return buf.toString();
  }
  
  public enum METADATA_FETCH_MODE {
    LOCAL,
    REMOTE
  }

  public static void checkInterrupt(String callerClassName) throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException(callerClassName + " execution is interrupted.");
    }
  }

  /**
   * Check for that special case when minor compaction is supported or not.
   * <ul>
   *   <li>The table is Insert-only OR</li>
   *   <li>Query based compaction is not enabled OR</li>
   *   <li>The table has only acid data in it.</li>
   * </ul>
   * @param tblproperties The properties of the table to check
   * @param dir The {@link AcidDirectory} instance pointing to the table's folder on the filesystem.
   * @return Returns true if minor compaction is supported based on the given parameters, false otherwise.
   */
  public static boolean isMinorCompactionSupported(HiveConf conf, Map<String, String> tblproperties, AcidDirectory dir) {
    //Query based Minor compaction is not possible for full acid tables having raw format (non-acid) data in them.
    return AcidUtils.isInsertOnlyTable(tblproperties) || !conf.getBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED)
        || !(dir.getOriginalFiles().size() > 0 || dir.getCurrentDirectories().stream().anyMatch(AcidUtils.ParsedDelta::isRawFormat));
  }

  public static LockRequest createLockRequest(HiveConf conf, CompactionInfo ci, long txnId, LockType lockType, DataOperationType opType) {
    String agentInfo = Thread.currentThread().getName();
    LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
    requestBuilder.setUser(ci.runAs);
    requestBuilder.setTransactionId(txnId);

    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
        .setLock(lockType)
        .setOperationType(opType)
        .setDbName(ci.dbname)
        .setTableName(ci.tableName)
        .setIsTransactional(true);

    if (ci.partName != null) {
      lockCompBuilder.setPartitionName(ci.partName);
    }
    requestBuilder.addLockComponent(lockCompBuilder.build());

    requestBuilder.setZeroWaitReadEnabled(!conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK) ||
        !conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
    return requestBuilder.build();
  }
}
