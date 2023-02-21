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
package org.apache.hadoop.hive.ql.txn.compactor.handler;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.txn.compactor.CacheContainer;
import org.apache.hadoop.hive.ql.txn.compactor.CleaningRequest;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * An abstract class which defines the list of utility methods for performing cleanup activities.
 */
public abstract class CleaningRequestHandler<T extends CleaningRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(CleaningRequestHandler.class.getName());
  protected final TxnStore txnHandler;
  protected final HiveConf conf;
  protected final boolean metricsEnabled;
  protected final CacheContainer cacheContainer;

  CleaningRequestHandler(HiveConf conf, TxnStore txnHandler, CacheContainer cacheContainer, boolean metricsEnabled) {
    this.conf = conf;
    this.txnHandler = txnHandler;
    this.cacheContainer = cacheContainer;
    this.metricsEnabled = metricsEnabled;
  }

  public TxnStore getTxnHandler() {
    return txnHandler;
  }

  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  /**
   * Find the list of objects which are ready for cleaning.
   * @return Cleaning requests
   */
  public abstract List<T> findReadyToClean() throws MetaException;

  /**
   * Execute just before cleanup
   * @param cleaningRequest - Cleaning request
   */
  public abstract void beforeExecutingCleaningRequest(T cleaningRequest) throws MetaException;

  /**
   * Execute just after cleanup
   * @param cleaningRequest Cleaning request
   * @param deletedFiles List of deleted files
   * @return True if cleanup was successful, false otherwise
   * @throws MetaException
   */
  public abstract boolean afterExecutingCleaningRequest(T cleaningRequest, List<Path> deletedFiles) throws MetaException;

  /**
   * Execute in the event of failure
   * @param cleaningRequest Cleaning request
   * @param ex Failure exception
   * @throws MetaException
   */
  public abstract void failureExecutingCleaningRequest(T cleaningRequest, Exception ex) throws MetaException;

  public Table resolveTable(String dbName, String tableName) throws MetaException {
    try {
      return getMSForConf(conf).getTable(getDefaultCatalog(conf), dbName, tableName);
    } catch (MetaException e) {
      LOG.error("Unable to find table {}.{}, {}", dbName, tableName, e.getMessage());
      throw e;
    }
  }

  protected Partition resolvePartition(String dbName, String tableName, String partName) throws MetaException {
    if (partName != null) {
      List<Partition> parts;
      try {
        parts = CompactorUtil.getPartitionsByNames(conf, dbName, tableName, partName);
        if (parts == null || parts.isEmpty()) {
          // The partition got dropped before we went looking for it.
          return null;
        }
      } catch (Exception e) {
        LOG.error("Unable to find partition: {}.{}.{}", dbName, tableName, partName, e);
        throw e;
      }
      if (parts.size() != 1) {
        LOG.error("{}.{}.{} does not refer to a single partition. {}", dbName, tableName, partName,
                Arrays.toString(parts.toArray()));
        throw new MetaException(String.join("Too many partitions for : ", dbName, tableName, partName));
      }
      return parts.get(0);
    } else {
      return null;
    }
  }
}
