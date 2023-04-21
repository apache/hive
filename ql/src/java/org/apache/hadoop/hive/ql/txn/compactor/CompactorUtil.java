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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
}
