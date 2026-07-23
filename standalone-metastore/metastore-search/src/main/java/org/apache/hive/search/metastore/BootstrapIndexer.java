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
    LOG.info("Bootstrap indexer planned {} table(s) in {} batch(es) across {} database(s)",
        plan.plannedTableCount(), plan.batches().size(), databases.size());

    BlockingQueue<List<TableDocument>> indexQueue =
        new ArrayBlockingQueue<>(indexConfig.getBootstrapQueueDepth());
    AtomicReference<Exception> failure = new AtomicReference<>();
    AtomicLong indexedTables = new AtomicLong();
    CountDownLatch fetchDone = new CountDownLatch(plan.batches().size());

    Thread indexThread = startIndexConsumer(
        indexQueue, failure, indexedTables, plan.plannedTableCount());
    try (ExecutorService tableFetcher = Executors.newFixedThreadPool(indexConfig.getBootstrapFetchThreads(),
        r -> {
          Thread thread = new Thread(r, "Index-Bootstrap-Fetch");
          thread.setDaemon(true);
          return thread;
    })) {
      for (TableBatch batch : plan.batches()) {
        tableFetcher.submit(() -> fetchBatch(batch, indexQueue, failure, fetchDone));
      }
      awaitFetchCompletion(fetchDone, failure);
      indexQueue.put(END_OF_STREAM);
      indexThread.join();
      rethrowFailure(failure);
      indexer.flush(notificationId, true);
      indexer.syncBackup();
      long elapsed = System.currentTimeMillis() - start;
      LOG.info("Built index for {} tables in {} batch(es) over {}ms",
          indexer.writer().getDocStats().numDocs, plan.batches().size(), elapsed);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IndexIOException("Bootstrap indexing interrupted", e);
    } finally {
      indexThread.interrupt();
      indexThread.join(TimeUnit.SECONDS.toMillis(30));
    }
  }

  private BatchPlan planBatches(List<String> databases) throws Exception {
    int batchSize = indexConfig.getBootstrapBatchSize();
    List<TableBatch> planned = new ArrayList<>();
    int plannedTableCount = 0;
    for (String db : databases) {
      List<String> tableNames = client.getAllTables(db);
      plannedTableCount += tableNames.size();
      for (int idx = 0; idx < tableNames.size(); idx += batchSize) {
        int end = Math.min(idx + batchSize, tableNames.size());
        planned.add(new TableBatch(db, List.copyOf(tableNames.subList(idx, end))));
      }
    }
    return new BatchPlan(planned, plannedTableCount);
  }

  private void fetchBatch(TableBatch batch,
      BlockingQueue<List<TableDocument>> indexQueue,
      AtomicReference<Exception> failure,
      CountDownLatch fetchDone) {
    try {
      if (failure.get() != null) {
        return;
      }
      if (shareFetchClient) {
        fetchBatch(batch, indexQueue, failure, client);
        return;
      }
      try (IMetaStoreClient fetchClient = RetryingMetaStoreClient.getProxy(configuration, true)) {
        fetchBatch(batch, indexQueue, failure, fetchClient);
      }
    } catch (Exception e) {
      recordFailure(failure, e);
    } finally {
      fetchDone.countDown();
    }
  }

  private void fetchBatch(TableBatch batch,
      BlockingQueue<List<TableDocument>> indexQueue,
      AtomicReference<Exception> failure,
      IMetaStoreClient fetchClient) throws Exception {
    if (failure.get() != null) {
      return;
    }
    List<Table> tables =
        fetchClient.getTableObjectsByName(batch.database(), batch.tableNames());
    List<TableDocument> documents = new ArrayList<>(tables.size());
    for (Table table : tables) {
      documents.add(MetastoreTableMapper.fromTable(table, mapping));
    }
    indexQueue.put(documents);
  }

  private Thread startIndexConsumer(BlockingQueue<List<TableDocument>> indexQueue,
      AtomicReference<Exception> failure, AtomicLong indexedTables,
      int plannedTableCount) {
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
          long now = System.currentTimeMillis();
          if (now - lastProgressLog >= indexConfig.getBootstrapProgressIntervalMs()) {
            long elapsedMs = Math.max(1, now - bootstrapWriterStart);
            double tablesPerSec = indexed * 1000.0 / elapsedMs;
            long remainingMs = estimateRemainingMs(indexed, plannedTableCount, elapsedMs);
            double percent = plannedTableCount == 0 ? 100.0 : indexed * 100.0 / plannedTableCount;
            LOG.info(
                "Bootstrap progress: indexed {}/{} table(s) ({}%), ~{}/s, ~{} remaining",
                indexed,
                plannedTableCount,
                String.format("%.1f", percent),
                String.format("%.1f", tablesPerSec),
                formatRemaining(remainingMs));
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

  /** Returns milliseconds left, 0 when done, or -1 when throughput is not yet measurable. */
  static long estimateRemainingMs(long indexed, long total, long elapsedMs) {
    if (total <= 0 || indexed >= total) {
      return 0;
    }
    if (indexed <= 0 || elapsedMs <= 0) {
      return -1;
    }
    long remainingTables = total - indexed;
    double tablesPerSec = indexed * 1000.0 / elapsedMs;
    return Math.round(remainingTables * 1000.0 / tablesPerSec);
  }

  static String formatRemaining(long remainingMs) {
    if (remainingMs < 0) {
      return "unknown";
    }
    if (remainingMs == 0) {
      return "0s";
    }
    long seconds = (remainingMs + 999) / 1000;
    if (seconds < 60) {
      return seconds + "s";
    }
    long minutes = seconds / 60;
    seconds = seconds % 60;
    if (minutes < 60) {
      return seconds == 0 ? minutes + "m" : minutes + "m " + seconds + "s";
    }
    long hours = minutes / 60;
    minutes = minutes % 60;
    return minutes == 0 ? hours + "h" : hours + "h " + minutes + "m";
  }

  private record BatchPlan(List<TableBatch> batches, int plannedTableCount) {}

  private record TableBatch(String database, List<String> tableNames) {}
}
