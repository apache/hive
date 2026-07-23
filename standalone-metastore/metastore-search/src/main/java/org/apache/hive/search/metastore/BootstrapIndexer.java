/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.exception.IndexIOException;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.config.IndexOptions;
import org.apache.hive.search.index.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class BootstrapIndexer {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapIndexer.class);
  private static final List<TableDocument> END_OF_STREAM = List.of();
  private static final TableBatch END_OF_WORK = new TableBatch(null, List.of());

  private final Configuration configuration;
  private final IndexOptions indexConfig;
  private final IndexMapping mapping;
  private final Indexer indexer;
  private final IMetaStoreClient client;
  private final boolean shareFetchClient;

  /** Package-private for unit tests: fetch workers reuse the injected client. */
  BootstrapIndexer(Configuration configuration,
      IndexMapping mapping, Indexer indexer,
      IMetaStoreClient client, boolean shareFetchClient) {
    this.configuration = configuration;
    this.indexConfig = new IndexOptions(configuration);
    this.mapping = mapping;
    this.indexer = indexer;
    this.client = client;
    this.shareFetchClient = shareFetchClient;
  }

  void run(long notificationId) throws Exception {
    long start = System.currentTimeMillis();
    List<String> databases = client.getAllDatabases();
    BatchPlan plan = planBatches(databases);
    LOG.info("Bootstrap planned {} table(s) in {} batch(es) across {} database(s)",
        plan.expectedTableCount(), plan.batches().size(), databases.size());

    int fetchThreads = indexConfig.getBootstrapFetchThreads();
    BlockingQueue<TableBatch> workQueue =
        new ArrayBlockingQueue<>(Math.max(fetchThreads * 2, 8));
    BlockingQueue<List<TableDocument>> indexQueue =
        new ArrayBlockingQueue<>(indexConfig.getBootstrapQueueDepth());
    AtomicReference<Exception> failure = new AtomicReference<>();
    AtomicLong indexedTables = new AtomicLong();
    AtomicInteger completedBatches = new AtomicInteger();
    CountDownLatch fetchDone = new CountDownLatch(plan.batches().size());

    Thread indexThread = startIndexConsumer(indexQueue, failure, indexedTables, completedBatches);
    ExecutorService fetchPool = Executors.newFixedThreadPool(fetchThreads, r -> {
      Thread thread = new Thread(r, "Index-Bootstrap-Fetch");
      thread.setDaemon(true);
      return thread;
    });
    try {
      for (int i = 0; i < fetchThreads; i++) {
        fetchPool.submit(() -> fetchTableWorker(plan, workQueue, indexQueue, failure, fetchDone));
      }
      enqueueBatches(plan.batches(), workQueue, failure, fetchDone);
      awaitFetchCompletion(fetchDone, failure);
      indexQueue.put(END_OF_STREAM);
      indexThread.join();
      rethrowFailure(failure);
      validateBootstrapTableCount(plan.expectedTableCount().get());
      indexer.flush(notificationId, true);
      indexer.syncBackup();
      long elapsed = System.currentTimeMillis() - start;
      LOG.info("Built index for {} tables in {} batch(es) over {}ms",
          plan.expectedTableCount(), plan.batches().size(), elapsed);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IndexIOException("Bootstrap indexing interrupted", e);
    } finally {
      workQueue.offer(END_OF_WORK);
      fetchPool.shutdownNow();
      indexThread.interrupt();
      indexThread.join(TimeUnit.SECONDS.toMillis(30));
    }
  }

  private void enqueueBatches(List<TableBatch> batches, BlockingQueue<TableBatch> workQueue,
      AtomicReference<Exception> failure, CountDownLatch fetchDone) throws InterruptedException {
    int enqueued = 0;
    try {
      for (TableBatch batch : batches) {
        if (failure.get() != null) {
          break;
        }
        workQueue.put(batch);
        enqueued++;
      }
    } finally {
      for (int i = enqueued; i < batches.size(); i++) {
        fetchDone.countDown();
      }
      workQueue.put(END_OF_WORK);
    }
  }

  private BatchPlan planBatches(List<String> databases) throws Exception {
    int batchSize = indexConfig.getBootstrapBatchSize();
    List<TableBatch> planned = new ArrayList<>();
    for (String db : databases) {
      List<String> tableNames = client.getAllTables(db);
      for (int idx = 0; idx < tableNames.size(); idx += batchSize) {
        int end = Math.min(idx + batchSize, tableNames.size());
        planned.add(new TableBatch(db, List.copyOf(tableNames.subList(idx, end))));
      }
    }
    return new BatchPlan(new AtomicInteger(0), planned);
  }

  private void fetchTableWorker(BatchPlan plan, BlockingQueue<TableBatch> workQueue,
      BlockingQueue<List<TableDocument>> indexQueue, AtomicReference<Exception> failure,
      CountDownLatch fetchDone) {
    if (shareFetchClient) {
      runFetchLoop(client, plan, workQueue, indexQueue, failure, fetchDone);
      return;
    }
    try (IMetaStoreClient fetchClient = RetryingMetaStoreClient.getProxy(configuration, true)) {
      runFetchLoop(fetchClient, plan, workQueue, indexQueue, failure, fetchDone);
    } catch (Exception e) {
      recordFailure(failure, e);
    }
  }

  private void runFetchLoop(IMetaStoreClient fetchClient, BatchPlan plan,
      BlockingQueue<TableBatch> workQueue, BlockingQueue<List<TableDocument>> indexQueue,
      AtomicReference<Exception> failure, CountDownLatch fetchDone) {
    try {
      while (true) {
        TableBatch batch = workQueue.take();
        if (batch == END_OF_WORK) {
          workQueue.offer(END_OF_WORK);
          return;
        }
        try {
          if (failure.get() == null) {
            List<Table> tables =
                fetchClient.getTableObjectsByName(batch.database(), batch.tableNames());
            plan.expectedTableCount().addAndGet(tables.size());
            List<TableDocument> documents = new ArrayList<>(tables.size());
            for (Table table : tables) {
              documents.add(MetastoreTableMapper.fromTable(table, mapping));
            }
            indexQueue.put(documents);
          }
        } catch (Exception e) {
          recordFailure(failure, e);
        } finally {
          fetchDone.countDown();
        }
      }
    } catch (Exception e) {
      recordFailure(failure, e);
    }
  }

  private Thread startIndexConsumer(BlockingQueue<List<TableDocument>> indexQueue,
      AtomicReference<Exception> failure, AtomicLong indexedTables,
      AtomicInteger completedBatches) {
    Thread indexThread = new Thread(() -> {
      long lastProgressLog = System.currentTimeMillis();
      long bootstrapWriterStart = System.currentTimeMillis();
      try {
        while (true) {
          List<TableDocument> documents = indexQueue.take();
          if (documents == END_OF_STREAM) {
            break;
          }
          if (failure.get() != null) {
            break;
          }
          indexer.addDocuments(documents);
          long indexed = indexedTables.addAndGet(documents.size());
          int done = completedBatches.incrementAndGet();
          long now = System.currentTimeMillis();
          if (now - lastProgressLog >= indexConfig.getBootstrapProgressIntervalMs()) {
            double tablesPerSec = indexed * 1000.0 / Math.max(1, now - bootstrapWriterStart);
            LOG.info("Bootstrap progress: indexed {} table(s) in {} batch(es), ~{}/s",
             indexed, done, String.format("%.1f", tablesPerSec));
            lastProgressLog = now;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        recordFailure(failure, e);
      } catch (Exception e) {
        recordFailure(failure, e);
      }
    }, "Index-Bootstrap-Writer");
    indexThread.setDaemon(true);
    indexThread.start();
    return indexThread;
  }

  private static void awaitFetchCompletion(CountDownLatch fetchDone,
      AtomicReference<Exception> failure) throws Exception {
    while (fetchDone.getCount() > 0) {
      if (failure.get() != null) {
        break;
      }
      fetchDone.await(1, TimeUnit.SECONDS);
    }
    rethrowFailure(failure);
  }

  private void validateBootstrapTableCount(int expectedTableCount) throws IndexIOException {
    int indexed = indexer.writer().getDocStats().numDocs;
    if (indexed != expectedTableCount) {
      throw new IndexIOException(
          "Bootstrap index doc count mismatch: indexed " + indexed
              + " documents but metastore has " + expectedTableCount + " tables");
    }
  }

  private static void recordFailure(AtomicReference<Exception> failure, Exception error) {
    failure.compareAndSet(null, error);
    LOG.error("Bootstrap indexing failed", error);
  }

  private static void rethrowFailure(AtomicReference<Exception> failure) throws Exception {
    Exception error = failure.get();
    if (error != null) {
      if (error instanceof IOException ioException) {
        throw ioException;
      }
      if (error instanceof InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        throw interruptedException;
      }
      throw new IndexIOException("Bootstrap indexing failed", error);
    }
  }

  private record BatchPlan(AtomicInteger expectedTableCount, List<TableBatch> batches) {}

  private record TableBatch(String database, List<String> tableNames) {}
}
