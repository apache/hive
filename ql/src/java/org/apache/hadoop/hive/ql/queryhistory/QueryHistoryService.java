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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.queryhistory.repository.QueryHistoryRepository;
import org.apache.hadoop.hive.ql.queryhistory.schema.Record;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.Time;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QueryHistoryService {
  private static final Logger LOG = LoggerFactory.getLogger(QueryHistoryService.class);

  private static QueryHistoryService instance = null;

  protected final Queue<Record> queryHistoryQueue = new LinkedBlockingQueue<>();

  private ExecutorService flushExecutor;
  private final ScheduledExecutorService periodicFlushExecutor = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setNameFormat("QueryHistoryService periodic flush check").setDaemon(true).build());

  private final HiveConf conf;
  private final ServiceContext serviceContext;

  private final int batchSize;
  private final int maxQueueSizeInMemoryBytes;
  private final int flushInterval;
  private final QueryHistoryRepository repository;

  public static QueryHistoryService newInstance(HiveConf inputConf, ServiceContext serviceContext) {
    // keeping a global, static instance is usually considered an antipattern, even in case of using the Singleton
    // design, this time the constraint was the ability to hook into the query processing (Driver) from somewhere the
    // outer service (HiveServer2, QTestUtil, etc.), which otherwise would have needed passing a service instance
    // through many layers, making the code and method signatures more complicated
    if (instance == null) {
      instance = new QueryHistoryService(inputConf, serviceContext);
    }
    return instance;
  }

  @VisibleForTesting
  QueryHistoryService(HiveConf inputConf, ServiceContext serviceContext) {
    LOG.info("Creating QueryHistoryService instance");

    this.conf = new HiveConf(inputConf);
    this.serviceContext = serviceContext;

    batchSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE);
    maxQueueSizeInMemoryBytes = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_QUERY_HISTORY_MAX_MEMORY_BYTES);
    flushInterval = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_QUERY_HISTORY_FLUSH_INTERVAL_SECONDS);
    repository = createRepository(conf);

    printConfigInformation();

    LOG.info("QueryHistoryService created [batchSize: {}, maxQueueSizeInMemoryBytes: {}]",
        batchSize, maxQueueSizeInMemoryBytes);
  }

  private static class QueryHistoryThread extends Thread {

    private final HiveConf conf;

    public QueryHistoryThread(Runnable runnable, HiveConf conf) {
      super(runnable);
      // this session won't need tez, let's not mess with starting a TezSessionState
      conf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);
      this.conf = conf;
    }

    @Override
    public void run() {
      // localize this conf for the thread that will be running the repository,
      // as iceberg integration heavily relies on the session state conf
      SessionState session = SessionState.start(conf);
      LOG.info("Session for QueryHistoryService started, sessionId: {}", session.getSessionId());
      super.run();
    }
  }

  /**
   * Start method actually starts the service in a sense that it initializes the repository (which might involve
   * database operations) and starts the executor that's responsible for flushing records.
   */
  public void start() {
    repository.init(conf, new Schema());

    flushExecutor = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("QueryHistoryService repository thread").setDaemon(true)
            .setThreadFactory(r -> new QueryHistoryThread(r, conf)).build());

    if (flushInterval > 0) {
      // only flush, don't wait, this is a background task
      periodicFlushExecutor.scheduleAtFixedRate(this::doFlush, 0, flushInterval, TimeUnit.SECONDS);
    }
  }

  public static QueryHistoryService getInstance() {
    if (instance == null) {
      throw new RuntimeException("QueryHistoryService is not present when asked for the singleton instance. " +
              "This is not expected to happen if 'hive.query.history.enabled' is properly set in the HS2 config. ");
    }
    return instance;
  }

  private void printConfigInformation() {
    if (batchSize == 0) {
      LOG.info("Query History batch size is 0: every record will be flushed synchronously after the query" +
          " (not recommended)");
    } else {
      LOG.info("Query History batch size is {}: records will be flushed after every {} queries",
          batchSize, batchSize);
    }
    if (maxQueueSizeInMemoryBytes == 0) {
      LOG.info("Query History max queue memory byte size is 0: the service won't check the consumed memory to make " +
          "flush decision (not recommended)");
    } else {
      LOG.info("Query History max queue memory byte size is {}: records will be flushed when the queue consumes " +
              "more than {} bytes in the memory",
          maxQueueSizeInMemoryBytes, maxQueueSizeInMemoryBytes);
    }

    if (flushInterval == 0) {
      LOG.info("Query History flush interval size is 0: the service won't check for records to flush periodically");
    } else {
      LOG.info("Query History flush interval is {}s: service will attempt to flush records in every {}s",
          flushInterval, flushInterval);
    }
  }

  private QueryHistoryRepository createRepository(HiveConf conf) {
    try {
      return (QueryHistoryRepository) ReflectionUtil.newInstance(
          conf.getClassByName(conf.get(HiveConf.ConfVars.HIVE_QUERY_HISTORY_REPOSITORY_CLASS.varname)), conf);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a Query History record from a given DriverContext and adds it to the queue.
   * This is done asynchronously to completely decouple from the threads that run the query to make this service work
   * silently in the background.
   * @param driverContext a DriverContext containing all the needed information for a record to be created
   */
  public void logQuery(DriverContext driverContext) {
    long recordCreateStart = Time.monotonicNow();
    Record record = createRecord(driverContext);
    long recordCreateEnd = Time.monotonicNow();
    queryHistoryQueue.add(record);
    LOG.info("Created query history record in {}ms, current queue size: {}",
        recordCreateEnd - recordCreateStart, queryHistoryQueue.size());

    CompletableFuture<?> flushFuture = flushRecordsIfNeeded();
    waitForRecordsToBeFlushed(batchSize == 0, flushFuture);
  }

  private CompletableFuture<?> flushRecordsIfNeeded() {
    if (needToFlush()) {
      return doFlush();
    } else {
      LOG.debug("Not flushing history records yet, current queue size: {}, batchSize: {}",
          queryHistoryQueue.size(), batchSize);
      return CompletableFuture.completedFuture(null);
    }
  }

  private void waitForRecordsToBeFlushed(boolean shouldWait, CompletableFuture<?> flushFuture) {
    if (shouldWait) {
      flushFuture.join();
    }
  }

  private CompletableFuture<Void> doFlush() {
    LOG.debug("Submitting a job to flush {} history records", queryHistoryQueue.size());
    return CompletableFuture.runAsync(() -> repository.flush(queryHistoryQueue), flushExecutor);
  }

  @VisibleForTesting
  boolean needToFlush() {
    boolean needToFlushAccordingToQueueSize = queryHistoryQueue.size() >= batchSize;
    if (needToFlushAccordingToQueueSize) {
      LOG.info("Need to flush query history records as queue size ({}) >= batch size ({})",
          queryHistoryQueue.size(), batchSize);
      return true;
    }

    // whether ignore the consumed memory or not
    if (maxQueueSizeInMemoryBytes == 0) {
      return false;
    }

    long queueMemorySize =
        queryHistoryQueue.stream().mapToLong(Record::getEstimatedSizeInMemoryBytes).sum();
    boolean needToFlushAccordingToQueueMemorySize = queueMemorySize > maxQueueSizeInMemoryBytes;
    if (needToFlushAccordingToQueueMemorySize) {
      LOG.info("Need to flush query history records as queue size in memory ({} bytes) is greater than the limit " +
              "({} bytes)",
          queueMemorySize, maxQueueSizeInMemoryBytes);
    }
    return needToFlushAccordingToQueueMemorySize;
  }

  public void stop() {
    LOG.info("Stopping QueryHistoryService, leftover records to flush: {}", queryHistoryQueue.size());

    // start periodic check first, we don't need it anymore
    periodicFlushExecutor.shutdownNow();

    CompletableFuture<?> flushFuture = doFlush();
    waitForRecordsToBeFlushed(true, flushFuture);
    flushExecutor.shutdownNow();
  }

  private Record createRecord(DriverContext driverContext) {
    return new RecordEnricher(driverContext, serviceContext, SessionState.get(),
        SessionState.getPerfLogger()).createRecord();
  }

  @VisibleForTesting
  public QueryHistoryRepository getRepository(){
    return repository;
  }
}
