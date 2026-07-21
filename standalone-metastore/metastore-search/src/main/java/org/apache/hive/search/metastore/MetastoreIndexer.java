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
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.leader.LeaderElection;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.index.Indexer;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.index.manifest.IndexManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetastoreIndexer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreIndexer.class);

  private final IMetaStoreClient client;
  private final long lastEventId;
  private final Indexer indexer;
  private final MetastoreCluster cluster;
  private final MetastoreEventHandler handler;
  private final IndexManager indexManager;
  private final FlushIndexListener flushIndexListener;
  private final boolean shareBootstrapFetchClient;

  public MetastoreIndexer(Configuration configuration, IndexManager indexManager, Indexer indexer)
      throws Exception {
    this(configuration, indexManager, indexer,
        RetryingMetaStoreClient.getProxy(configuration, true), false);
  }

  MetastoreIndexer(Configuration configuration, IndexManager indexManager, Indexer indexer,
      IMetaStoreClient client) throws Exception {
    this(configuration, indexManager, indexer, client, true);
  }

  MetastoreIndexer(Configuration configuration, IndexManager indexManager, Indexer indexer,
      IMetaStoreClient client, boolean shareBootstrapFetchClient) throws Exception {
    this.client = client;
    this.shareBootstrapFetchClient = shareBootstrapFetchClient;
    this.indexer = indexer;
    this.indexManager = indexManager;
    this.flushIndexListener = new FlushIndexListener(configuration);
    this.handler = new MetastoreEventHandler(configuration, client);
    this.handler.addListeners(flushIndexListener, indexManager);
    this.cluster = new MetastoreCluster(configuration, flushIndexListener);
    this.lastEventId = initialize();
    this.indexManager.setIndexedNid(lastEventId);
  }

  private boolean rebuildIndex() throws Exception {
    indexManager.resolveInterruptedRestore();
    if (isIndexValid(indexManager.readLocalManifest().orElse(null))) {
      return false;
    }
    if (isIndexValid(indexManager.readRemoteManifest().orElse(null))) {
      if (indexManager.restoreBackup()) {
        return false;
      }
      LOG.warn("Failed to restore index from remote backup; clearing partial local index");
      indexManager.clearLocalIndex();
      return true;
    }
    indexManager.clearLocalIndex();
    return true;
  }

  private boolean isIndexValid(IndexManifest indexManifest)
      throws Exception {
    if (indexManifest == null) {
      return false;
    }
    String currentModelName = indexManager.mapping().inference().modelName();
    String modelName = indexManifest.modelName();
    if (!currentModelName.equals(modelName)) {
      return false;
    }
    long notificationId = indexManifest.lastEventId();
    return canCatchUp(notificationId);
  }

  private boolean canCatchUp(long notificationId) throws Exception {
    if (notificationId < 0) {
      return false;
    }
    try {
      NotificationEventRequest request = new NotificationEventRequest(notificationId);
      request.setMaxEvents(10);
      client.getNextNotification(request, false, null);
      return true;
    } catch (IllegalStateException ignored) {
      LOG.debug("Current index lags too behind, will rebuild");
    }
    return false;
  }

  private long initialize() throws Exception {
    boolean rebuild = rebuildIndex();
    if (rebuild && !cluster.isLeader()) {
      long notificationId = waitForRemoteBackup();
      indexer.initialize();
      if (notificationId >= 0) {
        return notificationId;
      }
      return rebuildLeaderIndex();
    }
    indexer.initialize();
    if (rebuild) {
      return rebuildLeaderIndex();
    }
    return indexManager.readLocalManifest().get().lastEventId();
  }

  private long rebuildLeaderIndex() throws Exception {
    indexManager.clearRemoteIndex();
    long notificationId = client.getCurrentNotificationEventId().getEventId();
    new BootstrapIndexer(
        indexManager.mapping().configuration(),
        indexManager.mapping(),
        indexer,
        client,
        shareBootstrapFetchClient).run(notificationId);
    return notificationId;
  }

  public void start() throws Exception {
    handler.start(lastEventId);
    flushIndexListener.start(lastEventId);
  }

  /** Package-private for integration tests. */
  int pollEvents(int count) throws IndexNotHealthyException {
    return handler.getNextMetastoreEvents(count);
  }

  /** Package-private for integration tests. */
  void flushCheckpoint() throws IOException {
    try {
      indexer.flush(client.getCurrentNotificationEventId().getEventId(), true);
    } catch (Exception e) {
      if (e instanceof IOException ioException) {
        throw ioException;
      }
      throw new IOException("Failed to flush index checkpoint", e);
    }
  }

  /** Package-private for integration tests. */
  void syncBackup() throws IOException {
    indexer.syncBackup();
  }

  /**
   * Wait for a follower-ready remote backup and restore it before the IndexWriter is opened.
   *
   * @return restored notification id, or -1 if this instance became leader while waiting
   */
  private long waitForRemoteBackup() throws Exception {
    String currentModelName = indexManager.mapping().inference().modelName();
    for (; !cluster.isLeader(); Thread.sleep(3000)) {
      Optional<IndexManifest> indexManifest = indexManager.readRemoteManifest();
      if (indexManifest.isEmpty()) {
        continue;
      }
      IndexManifest manifest = indexManifest.get();
      if (!currentModelName.equals(manifest.modelName())) {
        LOG.debug("Remote index model {} does not match configured model {}, waiting",
            manifest.modelName(), currentModelName);
        continue;
      }
      long notificationId = manifest.lastEventId();
      if (notificationId > 0 && canCatchUp(notificationId)) {
        if (indexManager.restoreBackup()) {
          return notificationId;
        }
        LOG.warn("Failed to restore the index from remote directory, will retry");
      }
    }
    indexManager.clearLocalIndex();
    return -1;
  }

  @Override
  public void close() throws Exception {
    try {
      cluster.close();
      handler.close();
    } finally {
      flushIndexListener.close();
      client.close();
    }
  }

  private class FlushIndexListener
      implements MetastoreEventListener,
      LeaderElection.LeadershipStateListener,
      AutoCloseable {
    private volatile long eventId;
    private volatile long lastCommittedEventId;
    private volatile boolean isLeader;
    private volatile Thread replicateThread;
    private volatile boolean started = false;
    private final Thread commitThread;
    private final IndexConfig indexConfig;

    public FlushIndexListener(Configuration configuration) {
      this.indexConfig = new IndexConfig(configuration);
      this.commitThread = getIndexCommitThread();
    }

    public void start(long initialEventId) {
      this.eventId = initialEventId;
      this.lastCommittedEventId = initialEventId;
      this.commitThread.start();
      this.started = true;
    }

    private Thread getIndexCommitThread() {
      Thread commitThread = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(indexConfig.getFlushInterval());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
          if (eventId <= lastCommittedEventId) {
            continue;
          }
          try {
            if (indexer.flush(eventId, false)) {
              lastCommittedEventId = eventId;
            } else if (eventId - lastCommittedEventId > indexConfig.getForceFlushEventGap()) {
              // Many events processed with no Lucene changes; advance checkpoint metadata.
              if (indexer.flush(eventId, true)) {
                lastCommittedEventId = eventId;
              }
            }
          } catch (IOException e) {
            LOG.warn("Error flushing the index", e);
          }
        }
      });
      commitThread.setName("Index-Commit");
      commitThread.setDaemon(true);
      return commitThread;
    }

    private Thread getIndexReplicateThread() {
      Thread replicateThread = new Thread(() -> {
        while (isLeader && !Thread.currentThread().isInterrupted()) {
          long interval = 10000;
          if (started) {
            try {
              indexer.syncBackup();
              interval = indexConfig.getSyncInterval();
            } catch (IOException e) {
              LOG.warn("Error replicating the index to remote directory", e);
            }
          }
          try {
            Thread.sleep(interval);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      });
      replicateThread.setDaemon(true);
      replicateThread.setName("Index-Replica");
      return replicateThread;
    }

    @Override
    public void takeLeadership(LeaderElection leaderElection) {
      isLeader = true;
      if (indexManager.hasBackup()) {
        if (replicateThread != null && replicateThread.isAlive()) {
          return;
        }
        replicateThread = getIndexReplicateThread();
        replicateThread.start();
      }
    }

    @Override
    public void lossLeadership(LeaderElection leaderElection) {
      isLeader = false;
      if (replicateThread != null) {
        replicateThread.interrupt();
        replicateThread = null;
      }
    }

    @Override
    public void notifyIndexTask(IndexTask task) throws IOException {
      if (!task.databasesToDrop.isEmpty()) {
        indexer.deleteDatabases(task.databasesToDrop.toArray(new DatabaseName[0]));
      }
      if (!task.tablesToDrop.isEmpty()) {
        String[] docIds = task.tablesToDrop.stream()
            .map(MetastoreTableMapper::tableId).toList().toArray(new String[0]);
        indexer.delete(docIds);
      }
      if (!task.tablesToAdd.isEmpty()) {
        List<TableDocument> newDocs = task.tablesToAdd.values().stream()
            .map(t -> MetastoreTableMapper.fromTable(t, indexManager.mapping())).toList();
        indexer.addDocuments(newDocs);
      }
      eventId = task.lastEventId;
    }

    @Override
    public void close() throws Exception {
      isLeader = false;
      try {
        if (replicateThread != null) {
          replicateThread.interrupt();
          replicateThread = null;
        }
        commitThread.interrupt();
      } finally {
        if (eventId > lastCommittedEventId) {
          indexer.flush(eventId, true);
          lastCommittedEventId = eventId;
        }
      }
    }
  }
}
