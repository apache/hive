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

package org.apache.hive.search.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.index.store.IndexManifest;
import org.apache.hive.search.exception.IndexIOException;
import org.apache.hive.search.index.store.LocalStateClient;
import org.apache.hive.search.index.store.IndexBackupUtils;
import org.apache.hive.search.index.store.IndexStateClient;
import org.apache.hive.search.config.IndexStoreConfig;
import org.apache.hive.search.metastore.MetastoreEventListener;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexManager implements AutoCloseable, MetastoreEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(IndexManager.class);
  private final IndexMapping mapping;
  private final Directory directory;
  private final IndexStateClient localIndex;
  private final IndexStateClient remoteIndex;
  private volatile IndexNotHealthyException exception;
  private volatile long processedEventId;
  private volatile long committedEventId;

  public IndexManager(IndexMapping mapping, Directory directory, IndexStateClient localIndex,
      IndexStateClient remoteIndex) {
    this.mapping = mapping;
    this.directory = directory;
    this.localIndex = localIndex;
    this.remoteIndex = remoteIndex;
  }

  public static IndexManager open(IndexMapping mapping, Configuration conf) throws IOException {
    IndexStoreConfig store = mapping.store();
    Directory directory;
    if (store.useMemory()) {
      directory = new ByteBuffersDirectory();
    } else {
      directory = openDiskDirectory(mapping.indexName(), store.getLocalPath());
    }

    IndexStateClient local =
        new LocalStateClient(directory, mapping.indexName());
    IndexStateClient remote = null;
    if (store.hasRemote()) {
      remote = IndexBackupUtils.openRemote(store.getRemoteUri(), mapping.indexName(), conf);
    }
    return new IndexManager(mapping, directory, local, remote);
  }

  private static Directory openDiskDirectory(String indexName, Path basePath) throws IOException {
    Path indexPath = basePath.resolve(indexName);
    Files.createDirectories(indexPath);
    return new MMapDirectory(indexPath);
  }

  public boolean hasBackup() {
    return remoteIndex != null;
  }

  public boolean syncBackup() throws IOException {
    if (remoteIndex == null) {
      return false;
    }
    if (localIndex.readManifest().isEmpty()) {
      return false;
    }
    return IndexBackupUtils.syncToBackup(localIndex, remoteIndex);
  }

  public boolean restoreBackup() {
    try {
      if (remoteIndex == null) {
        return false;
      }
      return IndexBackupUtils.restoreFromBackup(localIndex, remoteIndex);
    } catch (IOException e) {
      LOG.warn("Cannot restore the index from remote backup", e);
      return false;
    }
  }

  public void resolveInterruptedRestore() {
    try {
      IndexBackupUtils.resolveInterruptedRestore(localIndex);
    } catch (IOException e) {
      LOG.warn("Cannot resolve interrupted local index restore", e);
    }
  }

  public Optional<IndexManifest> readLocalManifest() {
    try {
      return localIndex.readManifest();
    } catch (IOException e) {
      LOG.warn("Cannot read the local index manifest", e);
      return Optional.empty();
    }
  }

  public Optional<IndexManifest> readRemoteManifest() {
    try {
      if (remoteIndex == null) {
        return Optional.empty();
      }
      return remoteIndex.readManifest();
    } catch (IOException e) {
      LOG.warn("Cannot read the remote index manifest", e);
      return Optional.empty();
    }
  }

  public void clearLocalIndex() throws IOException {
    localIndex.clear();
  }

  public void clearRemoteIndex() throws IOException {
    if (remoteIndex != null) {
      remoteIndex.clear();
    }
  }

  public void notifyIndexState(boolean healthy, IndexNotHealthyException... e) {
    if (healthy) {
      exception = null;
      return;
    }
    if (e.length > 0 && e[0] != null) {
      exception = e[0];
    }
  }

  @Override
  public void notifyIndexTask(IndexTask task) {
    processedEventId = task.lastEventId;
  }

  public void setCommittedEventId(long eventId) {
    this.committedEventId = eventId;
  }

  public long getProcessedEventId() {
    return processedEventId;
  }

  public long getCommittedEventId() {
    return committedEventId;
  }

  public void setNid(long nid) {
    this.committedEventId = nid;
    this.processedEventId = nid;
  }

  public void checkIndexState() throws IndexNotHealthyException {
    if (exception != null) {
      throw exception;
    }
  }

  public IndexMapping mapping() {
    return mapping;
  }

  public Directory directory() {
    return directory;
  }

  @Override
  public void close() throws IOException {
    directory.close();
    if (remoteIndex instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        if (e instanceof IOException ioe) {
          throw ioe;
        }
        throw new IndexIOException("Failed to close remote index client", e);
      }
    }
  }
}
