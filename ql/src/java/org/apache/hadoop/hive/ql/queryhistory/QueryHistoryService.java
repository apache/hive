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

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.queryhistory.repository.QueryHistoryRepository;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.Time;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class QueryHistoryService {
  private static final Logger LOG = LoggerFactory.getLogger(QueryHistoryService.class);

  private static QueryHistoryService INSTANCE = null;

  protected final Queue<QueryHistoryRecord> queryHistoryQueue = new LinkedBlockingQueue<>();

  private final ExecutorService persistExecutor = Executors.newFixedThreadPool(1,
      new ThreadFactoryBuilder().setNameFormat("QueryHistoryService repository thread").setDaemon(true).build());

  private final HiveConf conf;
  private final ServiceContext serviceContext;

  private final int maxBatchSize;
  private final int maxQueueSizeInMemoryBytes;
  private final QueryHistoryRepository repository;

  public QueryHistoryService(HiveConf inputConf, ServiceContext serviceContext) {
    LOG.info("Creating QueryHistoryService instance");

    this.conf = new HiveConf(inputConf);
    this.serviceContext = serviceContext;

    maxBatchSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_QUERY_HISTORY_PERSIST_MAX_BATCH_SIZE);
    maxQueueSizeInMemoryBytes = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_QUERY_HISTORY_PERSIST_MAX_MEMORY_BYTES);

    repository = createRepository(conf);

    printConfigInformation();

    LOG.info("QueryHistoryService created [maxBatchSize: {}, maxQueueSizeInMemoryBytes: {}]",
        maxBatchSize, maxQueueSizeInMemoryBytes);
  }

  /**
   * Start method actually starts the service in a sense that it initializes the repository (which might involve
   * database operations) and starts the executor that's responsible for persisting records.
   * @return the service instance
   */
  public QueryHistoryService start() {
    repository.init(conf, new QueryHistorySchema());

    try {
      persistExecutor.submit(() -> {
        // this session won't need tez, let's not mess with starting a TezSessionState
        conf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);

        // localize this conf for the thread that will be running the repository,
        // as iceberg integration heavily relies on the session state conf
        SessionState session = SessionState.start(conf);
        LOG.info("Session for QueryHistoryService started, sessionId: {}", session.getSessionId());
      });
    } catch (Exception e) {
      throw new RuntimeException("Failed to start QueryHistoryService", e);
    }

    return this;
  }

  public static QueryHistoryService getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("QueryHistoryService is not present when asked for the singleton instance");
    }
    return INSTANCE;
  }

  // keeping a global, static instance is usually considered an antipattern, even in case of using the Singleton
  // design, this time the constraint was the ability to hook into the query processing (Driver) from somewhere the
  // outer service (HiveServer2, QTestUtil, etc.), which otherwise would have needed passing a service instance
  // through many layers, making the code and method signatures more complicated
  public static void setInstance(QueryHistoryService queryHistoryService) {
    INSTANCE = queryHistoryService;
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

  protected QueryHistoryRepository createRepository(HiveConf conf) {
    initializeSessionForTableCreation(conf);

    try {
      return (QueryHistoryRepository) ReflectionUtil.newInstance(
          conf.getClassByName(conf.get(HiveConf.ConfVars.HIVE_QUERY_HISTORY_REPOSITORY_CLASS.varname)), conf);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeSessionForTableCreation(HiveConf conf) {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, QueryHistoryRepository.QUERY_ID_FOR_TABLE_CREATION);
    SessionState ss = SessionState.get(); // if there is a SessionState in the called thread, we can use that
    if (ss == null){
      ss = SessionState.start(conf);
    }
    ss.addQueryState(QueryHistoryRepository.QUERY_ID_FOR_TABLE_CREATION,
        new QueryState.Builder().withHiveConf(conf).build());
  }

  /**
   * Creates a Query History record from a given DriverContext and adds it to the queue.
   * This is done asynchronously to completely decouple from the threads that run the query to make this service work
   * silently in the background.
   * @param driverContext a DriverContext containing all the needed information for a record to be created
   */
  public void handleQuery(DriverContext driverContext) {
    long recordCreateStart = Time.monotonicNow();
    QueryHistoryRecord record = createRecord(driverContext);
    long recordCreateEnd = Time.monotonicNow();
    queryHistoryQueue.add(record);
    LOG.info("Created query history record in {}ms, current queue size: {}",
        recordCreateEnd - recordCreateStart, queryHistoryQueue.size());

    persistRecordsIfNeeded();
  }

  private void persistRecordsIfNeeded() {
    if (needToPersist()) {
      persistWithBatchSize(maxBatchSize);
    } else {
      LOG.debug("Not persisting history records yet, current queue size: {}, maxBatchSize: {}",
          queryHistoryQueue.size(), maxBatchSize);
    }
  }

  private void persistWithBatchSize(int batchSize) {
    CountDownLatch latch = new CountDownLatch(1);
    LOG.debug("Submitting a job to persist {} history records", queryHistoryQueue.size());
    persistExecutor.submit(() -> {
      repository.persist(queryHistoryQueue);
      latch.countDown();
    });
    if (batchSize == 0) {
      waitForAllRecordsToBePersisted(latch);
    }
  }

  private void waitForAllRecordsToBePersisted(CountDownLatch queueEmptyCondition) {
    try {
      queueEmptyCondition.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
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
    persistWithBatchSize(0); // persist sync
    persistExecutor.shutdownNow();
  }

  private QueryHistoryRecord createRecord(DriverContext driverContext) {
    return new QueryHistoryRecordEnricher(driverContext, serviceContext, SessionState.get(),
        SessionState.getPerfLogger()).createRecord();
  }

  @VisibleForTesting
  public QueryHistoryRepository getRepository(){
    return repository;
  }

  @Override
  public String toString() {
    return String.format("[QueryHistoryService @%s, maxBatchSize: %d, maxQueueSizeInMemoryBytes: %d]",
        Integer.toHexString(hashCode()), maxBatchSize, maxQueueSizeInMemoryBytes);
  }
}
