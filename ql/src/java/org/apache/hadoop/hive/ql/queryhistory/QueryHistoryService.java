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
package org.apache.hadoop.hive.ql.queryhistory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.queryhistory.persist.QueryHistoryPersistor;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class QueryHistoryService {
  private static final Logger LOG = LoggerFactory.getLogger(QueryHistoryService.class);

  private static QueryHistoryService INSTANCE = null;

  private QueryHistoryPersistor queryHistoryPersistor;

  protected final Queue<QueryHistoryRecord> queryHistoryQueue = new LinkedBlockingQueue<>();
  private int maxBatchSize = 0;
  private int maxQueueSizeInMemoryBytes = 0;

  private final ExecutorService persistExecutor = Executors.newFixedThreadPool(1,
      new ThreadFactoryBuilder().setNameFormat("QueryHistoryService persistor thread").build());

  private ServiceContext serviceContext;

  public static QueryHistoryService start(HiveConf inputConf, ServiceContext serviceContext) {
    HiveConf conf = new HiveConf(inputConf);
    // we can only hit this codepath if the INSTANCE is already initialized while calling start(...)
    // for convenient test compatibility, this call is allowed to be idempotent (LOG.warn instead of exception)
    if (INSTANCE != null) {
      LOG.warn("There is already a QueryHistoryService instance ({}), returning existing instance", INSTANCE);
      return INSTANCE;
    }

    INSTANCE = createServiceInstance(serviceContext, conf);
    return INSTANCE;
  }

  private static QueryHistoryService createServiceInstance(ServiceContext serviceContext, HiveConf conf) {
    LOG.info("Starting QueryHistoryService");
    QueryHistoryService queryHistoryService = new QueryHistoryService();

    initService(serviceContext, conf, queryHistoryService);
    return queryHistoryService;
  }

  @VisibleForTesting
  static void initService(ServiceContext serviceContext, HiveConf conf, QueryHistoryService queryHistoryService) {
    queryHistoryService.queryHistoryPersistor = createPersistor(conf);
    queryHistoryService.queryHistoryPersistor.init(conf, new QueryHistorySchema());
    queryHistoryService.maxBatchSize = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_QUERY_HISTORY_SERVICE_PERSIST_MAX_BATCH_SIZE);
    queryHistoryService.maxQueueSizeInMemoryBytes = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_QUERY_HISTORY_SERVICE_PERSIST_MAX_MEMORY_BYTES);

    queryHistoryService.printConfigInformation();
    queryHistoryService.serviceContext = serviceContext;

    try {
      queryHistoryService.persistExecutor.submit(() -> {
        // this session won't need tez, let's not mess with starting a TezSessionState
        conf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);

        // localize this conf for the thread that will be running the persistor,
        // as iceberg integration heavily relies on the session state conf
        SessionState session = SessionState.start(conf);
        LOG.info("Session for QueryHistoryService started, sessionId: {}", session.getSessionId());
      });
    } catch (Exception e) {
      throw new RuntimeException("Failed to start QueryHistoryService", e);
    }
    LOG.info("QueryHistoryService started [maxBatchSize: {}, maxQueueSizeInMemoryBytes: {}]",
        queryHistoryService.maxBatchSize, queryHistoryService.maxQueueSizeInMemoryBytes);
  }

  private void printConfigInformation() {
    if (maxBatchSize == 0) {
      LOG.info("Query History batch size is 0: every record will be persisted synchronously after the query" +
          " (not recommended)");
    } else {
      LOG.info("Query History batch size is {}: records will be persisted after every {} queries",
          maxBatchSize, maxBatchSize);
    }
    if (maxQueueSizeInMemoryBytes == 0) {
      LOG.info("Query History max queue memory byte size is 0: the service won't check the consumed memory to make " +
          "persist decision (not recommended)");
    } else {
      LOG.info("Query History max queue memory byte size is {}: records will be persisted when the queue consumes " +
              "more than {} bytes in the memory",
          maxQueueSizeInMemoryBytes, maxQueueSizeInMemoryBytes);
    }
  }

  public static QueryHistoryPersistor createPersistor(HiveConf conf) {
    initializeSessionForTableCreation(conf);

    try {
      return (QueryHistoryPersistor) ReflectionUtil.newInstance(
          conf.getClassByName(conf.get(HiveConf.ConfVars.HIVE_QUERY_HISTORY_SERVICE_PERSISTOR_CLASS.varname)), conf);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static void initializeSessionForTableCreation(HiveConf conf) {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, QueryHistoryPersistor.QUERY_ID_FOR_TABLE_CREATION);
    SessionState ss = SessionState.get(); // if there is a SessionState in the called thread, we can use that
    if (ss == null){
      ss = SessionState.start(conf);
    }
    ss.addQueryState(QueryHistoryPersistor.QUERY_ID_FOR_TABLE_CREATION,
        new QueryState.Builder().withHiveConf(conf).build());
  }

  // keeping a global, static instance is usually considered an antipattern, even in case of using the Singleton
  // design, this time the constraint was the ability to hook into the query processing (Driver) from somewhere the
  // outer service (HiveServer2, QTestUtil, etc.), which otherwise would have needed passing a service instance
  // through many layers, making the code and method signatures more complicated
  public static void handle(DriverContext driverContext) {
    if (INSTANCE == null){
      // we can only hit this codepath if hive.query.history.service.enabled=true but the INSTANCE is not initialized,
      // which is unlikely to happen under a production HiveServer2, as it initializes the service properly
      LOG.warn("QueryHistoryService is called but not initialized, this is only fine under testing circumstances.");
      return;
    }
    INSTANCE.handleRecord(driverContext);
  }

  @VisibleForTesting
  protected QueryHistoryService() {
  }

  private void persistRecordsIfNeeded() {
    if (needToPersist()) {
      LOG.debug("Submitting a job to persist {} history records", queryHistoryQueue.size());
      persistExecutor.submit(() -> {
        queryHistoryPersistor.persist(queryHistoryQueue);
      });
      if (maxBatchSize == 0) {
        waitForAllRecordsToBePersisted();
      }
    } else {
      LOG.debug("Not persisting history records yet, current queue size: {}, maxBatchSize: {}",
          queryHistoryQueue.size(),
          maxBatchSize);
    }
  }

  private void waitForAllRecordsToBePersisted() {
    while (!queryHistoryQueue.isEmpty()){
      try {
        LOG.info("Waiting for {} records to persist synchronously", queryHistoryQueue.size());
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  boolean needToPersist() {
    boolean needToPersistAccordingToQueueSize = queryHistoryQueue.size() >= maxBatchSize;
    if (needToPersistAccordingToQueueSize) {
      LOG.info("Need to persist query history records as queue size ({}) >= batch size ({})",
          queryHistoryQueue.size(), maxBatchSize);
      return true;
    }

    // whether ignore the consumed memory or not
    if (maxQueueSizeInMemoryBytes == 0) {
      return false;
    }

    long queueMemorySize =
        queryHistoryQueue.stream().mapToLong(QueryHistoryRecord::getEstimatedSizeInMemoryBytes).sum();
    boolean needToPersistAccordingToQueueMemorySize = queueMemorySize > maxQueueSizeInMemoryBytes;
    if (needToPersistAccordingToQueueMemorySize) {
      LOG.info("Need to persist query history records as queue size in memory ({} bytes) is greater than the limit " +
              "({} bytes)",
          queueMemorySize, maxQueueSizeInMemoryBytes);
    }
    return needToPersistAccordingToQueueMemorySize;
  }

  public void stop() {
    LOG.info("Stopping QueryHistoryService, leftover records to persist: {}", queryHistoryQueue.size());
    persistSync();
  }

  private void persistSync() {
    persistExecutor.submit(() -> {
      queryHistoryPersistor.persist(queryHistoryQueue);
    });
    waitForAllRecordsToBePersisted();
  }

  /**
   * Creates a Query History record from a given DriverContext and adds it to the queue.
   * This is done asynchronously to completely decouple from the threads that run the query to make this service work
   * silently in the background.
   * @param driverContext a DriverContext containing all the needed information for a record to be created
   */
  private void handleRecord(DriverContext driverContext) {
    QueryHistoryRecord record = createRecord(driverContext);
    LOG.debug("Created history record: {}", record);

    queryHistoryQueue.add(record);
    LOG.info("Added history record, current queue size: {}", INSTANCE.queryHistoryQueue.size());

    persistRecordsIfNeeded();
  }

  private QueryHistoryRecord createRecord(DriverContext driverContext) {
    QueryHistoryRecord record = new QueryHistoryRecord();

    FromDriverContextUpdater.getInstance().consume(driverContext, record);
    FromSessionStateUpdater.getInstance().consume(SessionState.get(), record);
    FromPerfLoggerUpdater.getInstance().consume(SessionState.getPerfLogger(), record);
    FromServiceContextUpdater.getInstance().consume(serviceContext, record);

    return record;
  }

  @VisibleForTesting
  QueryHistoryPersistor getPersistor(){
    return queryHistoryPersistor;
  }

  @Override
  public String toString() {
    return String.format("[QueryHistoryService @%s, maxBatchSize: %d, maxQueueSizeInMemoryBytes: %d]",
        Integer.toHexString(hashCode()), maxBatchSize, maxQueueSizeInMemoryBytes);
  }
}
