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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.TimeValidator;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Partition management task is primarily responsible for partition retention and discovery based on table properties.
 *
 * Partition Retention - If "partition.retention.period" table property is set with retention interval, when this
 * metastore task runs periodically, it will drop partitions with age (creation time) greater than retention period.
 * Dropping partitions after retention period will also delete the data in that partition.
 *
 * Partition Discovery - If "discover.partitions" table property is set, this metastore task monitors table location
 * for newly added partition directories and create partition objects if it does not exist. Also, if partition object
 * exist and if corresponding directory does not exists under table location then the partition object will be dropped.
 *
 */
public class PartitionManagementTask implements MetastoreTaskThread {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManagementTask.class);
  public static final String DISCOVER_PARTITIONS_TBLPROPERTY = "discover.partitions";
  public static final String PARTITION_RETENTION_PERIOD_TBLPROPERTY = "partition.retention.period";
  private static final Lock lock = new ReentrantLock();
  // these are just for testing
  private static int completedAttempts;
  private static int skippedAttempts;

  private Configuration conf;

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TASK_FREQUENCY, unit);
  }

  @Override
  public void setConf(Configuration configuration) {
    // we modify conf in setupConf(), so we make a copy
    conf = new Configuration(configuration);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private static boolean partitionDiscoveryEnabled(Map<String, String> params) {
    return params != null && params.containsKey(DISCOVER_PARTITIONS_TBLPROPERTY) &&
            params.get(DISCOVER_PARTITIONS_TBLPROPERTY).equalsIgnoreCase("true");
  }

  @Override
  public void run() {
    if (lock.tryLock()) {
      skippedAttempts = 0;
      String qualifiedTableName = null;
      IMetaStoreClient msc = null;
      try {
        msc = new HiveMetaStoreClient(conf);
        String catalogName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PARTITION_MANAGEMENT_CATALOG_NAME);
        String dbPattern = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PARTITION_MANAGEMENT_DATABASE_PATTERN);
        String tablePattern = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_PATTERN);
        String tableTypes = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TABLE_TYPES);
        Set<String> tableTypesSet = new HashSet<>();
        for (String type : tableTypes.split(",")) {
          try {
            tableTypesSet.add(TableType.valueOf(type.trim().toUpperCase()).name());
          } catch (IllegalArgumentException e) {
            // ignore
            LOG.warn("Unknown table type: {}", type);
          }
        }
        // if tableTypes is empty, then a list with single empty string has to specified to scan no tables.
        // specifying empty here is equivalent to disabling the partition discovery altogether as it scans no tables.
        if (tableTypesSet.isEmpty()) {
          LOG.info("Skipping partition management as no table types specified");
          return;
        }

        StringBuilder filterBuilder = new StringBuilder()
            .append(hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS)
            .append("discover__partitions").append(" like \"true\" ");
        boolean external = tableTypesSet.contains(TableType.EXTERNAL_TABLE.name());
        boolean managed = tableTypesSet.contains(TableType.MANAGED_TABLE.name());
        if (!managed && external) {
          // only for external tables
          filterBuilder.append(" and ").append(hive_metastoreConstants.HIVE_FILTER_FIELD_TABLE_TYPE)
              .append(" = \"").append(TableType.EXTERNAL_TABLE.name()).append("\" ");
        } else if (managed && !external) {
          // only for managed tables
          filterBuilder.append(" and ").append(hive_metastoreConstants.HIVE_FILTER_FIELD_TABLE_TYPE)
              .append(" = \"").append(TableType.MANAGED_TABLE.name()).append("\" ");
        }
        if (!tablePattern.trim().isEmpty()) {
          filterBuilder.append(" and ")
              .append(hive_metastoreConstants.HIVE_FILTER_FIELD_TABLE_NAME)
              .append(" like \"").append(tablePattern.replaceAll("\\*", ".*")).append("\"");
        }

        List<String> databases = msc.getDatabases(catalogName, dbPattern);
        List<TableName> candidates = new ArrayList<>();
        for (String db : databases) {
          Database database = msc.getDatabase(catalogName, db);
          if (MetaStoreUtils.checkIfDbNeedsToBeSkipped(database)) {
            LOG.debug("Skipping table under database: {}", db);
            continue;
          }
          if (MetaStoreUtils.isDbBeingPlannedFailedOver(database)) {
            LOG.info("Skipping table belongs to database {} being failed over.", db);
            continue;
          }
          List<String> tablesNames = msc.listTableNamesByFilter(catalogName, db,
              filterBuilder.toString(), -1);
          tablesNames.forEach(tablesName -> candidates.add(TableName.fromString(tablesName, catalogName, db)));
        }

        if (candidates.isEmpty()) {
          LOG.info("Got empty table list in catalog: {}, dbPattern: {}", catalogName, dbPattern);
          return;
        }

        // TODO: Msck creates MetastoreClient (MSC) on its own. MSC creation is expensive. Sharing MSC also
        // will not be safe unless synchronized MSC is used. Using synchronized MSC in multi-threaded context also
        // defeats the purpose of thread pooled msck repair.
        int threadPoolSize = MetastoreConf.getIntVar(conf,
          MetastoreConf.ConfVars.PARTITION_MANAGEMENT_TASK_THREAD_POOL_SIZE);
        final ExecutorService executorService = Executors
          .newFixedThreadPool(Math.min(candidates.size(), threadPoolSize),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("PartitionDiscoveryTask-%d").build());
        CountDownLatch countDownLatch = new CountDownLatch(candidates.size());
        LOG.info("Found {} candidate tables for partition discovery", candidates.size());
        setupMsckPathInvalidation();
        Configuration msckConf = Msck.getMsckConf(conf);
        for (TableName table : candidates) {
          // this always runs in 'sync' mode where partitions can be added and dropped
          MsckInfo msckInfo = new MsckInfo(table.getCat(), table.getDb(), table.getTable(),
            null, null, true, true, true, -1);
          executorService.submit(new MsckThread(msckInfo, msckConf, qualifiedTableName, countDownLatch));
        }
        countDownLatch.await();
        executorService.shutdownNow();
      } catch (Exception e) {
        LOG.error("Exception while running partition discovery task for table: " + qualifiedTableName, e);
      } finally {
        if (msc != null) {
          msc.close();
        }
        lock.unlock();
      }
      completedAttempts++;
    } else {
      skippedAttempts++;
      LOG.info("Lock is held by some other partition discovery task. Skipping this attempt..#{}", skippedAttempts);
    }
  }

  public static long getRetentionPeriodInSeconds(final Table table) {
    String retentionPeriod;
    long retentionSeconds = -1;
    if (table.getParameters() != null && table.getParameters().containsKey(PARTITION_RETENTION_PERIOD_TBLPROPERTY)) {
      retentionPeriod = table.getParameters().get(PARTITION_RETENTION_PERIOD_TBLPROPERTY);
      if (retentionPeriod.isEmpty()) {
        LOG.warn("'{}' table property is defined but empty. Skipping retention period..",
          PARTITION_RETENTION_PERIOD_TBLPROPERTY);
      } else {
        try {
          TimeValidator timeValidator = new TimeValidator(TimeUnit.SECONDS);
          timeValidator.validate(retentionPeriod);
          retentionSeconds = MetastoreConf.convertTimeStr(retentionPeriod, TimeUnit.SECONDS, TimeUnit.SECONDS);
        } catch (IllegalArgumentException e) {
          LOG.warn("'{}' retentionPeriod value is invalid. Skipping retention period..", retentionPeriod);
          // will return -1
        }
      }
    }
    return retentionSeconds;
  }

  private void setupMsckPathInvalidation() {
    // if invalid partition directory appears, we just skip and move on. We don't want partition management to throw
    // when invalid path is encountered as these are background threads. We just want to skip and move on. Users will
    // have to fix the invalid paths via external means.
    conf.set(MetastoreConf.ConfVars.MSCK_PATH_VALIDATION.getVarname(), "skip");
  }

  private static class MsckThread implements Runnable {
    private MsckInfo msckInfo;
    private Configuration conf;
    private String qualifiedTableName;
    private CountDownLatch countDownLatch;

    MsckThread(MsckInfo msckInfo, Configuration conf, String qualifiedTableName, CountDownLatch countDownLatch) {
      this.msckInfo = msckInfo;
      this.conf = conf;
      this.qualifiedTableName = qualifiedTableName;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
      try {
        Msck msck = new Msck( true, true);
        msck.init(conf);
        msck.repair(msckInfo);
      } catch (Exception e) {
        LOG.error("Exception while running partition discovery task for table: " + qualifiedTableName, e);
      } finally {
        // there is no recovery from exception, so we always count down and retry in next attempt
        countDownLatch.countDown();
      }
    }
  }

  @VisibleForTesting
  public static int getSkippedAttempts() {
    return skippedAttempts;
  }

  @VisibleForTesting
  public static int getCompletedAttempts() {
    return completedAttempts;
  }
}
