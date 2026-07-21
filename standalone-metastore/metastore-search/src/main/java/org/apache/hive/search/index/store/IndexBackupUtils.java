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

package org.apache.hive.search.index.store;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.config.IndexStoreConfig;
import org.apache.hive.search.exception.IndexIOException;

public final class IndexBackupUtils {
  private IndexBackupUtils() {}

  /** Push local index state to the backup (remote) location. */
  public static boolean syncToBackup(IndexStateClient local, IndexStateClient remote)
      throws IOException {
    Optional<IndexManifest> localManifest = local.readManifest();
    if (localManifest.isEmpty()) {
      return false;
    }
    Optional<IndexManifest> remoteManifest = remote.readManifest();
    IndexManifest localM = localManifest.get();
    if (remoteManifest.isPresent()) {
      IndexManifest remoteM = remoteManifest.get();
      if (remoteM.lastEventId() > localM.lastEventId()) {
        return false;
      }
      if (remoteM.lastEventId() == localM.lastEventId() && localM.sameFilesAs(remoteM)) {
        return false;
      }
    }
    applyDiff(local, remote, localM.diff(remoteManifest.orElse(null)));
    return remote.writeManifest(localM);
  }

  /**
   * If a previous restore left a staging manifest, finalize it when files already match,
   * or keep it in place so {@link #restoreFromBackup} can resume.
   */
  public static void resolveInterruptedRestore(IndexStateClient local)
      throws IOException {
    Optional<IndexManifest> staging = local.readStagingManifest();
    if (staging.isEmpty()) {
      local.clearStagingManifest();
      return;
    }
    IndexManifest target = staging.get();
    if (target.sameFilesAs(local.readLocalFileManifest())) {
      local.validateRestoredIndex(target);
      local.clearStagingManifest();
    }
  }

  /** Pull backup (remote) index state into the local directory. */
  public static boolean restoreFromBackup(IndexStateClient local, IndexStateClient remote)
      throws IOException {
    Optional<IndexManifest> remoteManifest = remote.readManifest();
    if (remoteManifest.isEmpty()) {
      return false;
    }
    IndexManifest target = remoteManifest.get();
    Optional<IndexManifest> localManifest = local.readManifest();
    if (localManifest.isPresent()
        && localManifest.get().lastEventId() >= target.lastEventId()) {
      return false;
    }
    prepareStagingManifest(local, target);
    IndexManifest localFiles = local.readLocalFileManifest();
    applyDiff(remote, local, target.diff(localFiles));
    local.validateRestoredIndex(target);
    local.clearStagingManifest();
    return true;
  }

  private static void prepareStagingManifest(IndexStateClient local, IndexManifest target)
      throws IOException {
    Optional<IndexManifest> staging = local.readStagingManifest();
    if (staging.isPresent()
        && staging.get().lastEventId() == target.lastEventId()
        && staging.get().embedder().equals(target.embedder())) {
      return;
    }
    local.clearStagingManifest();
    local.writeStagingManifest(target);
  }

  private static void applyDiff(IndexStateClient source, IndexStateClient dest,
      List<IndexManifest.ChangedFileOp> ops) throws IOException {
    for (IndexManifest.ChangedFileOp op : ops) {
      switch (op) {
        case IndexManifest.ChangedFileOp.Add add -> {
          try (InputStream in = source.read(add.fileName())) {
            dest.write(add.fileName(), in);
          }
        }
        case IndexManifest.ChangedFileOp.Del del -> dest.delete(del.fileName());
        default -> throw new IndexIOException("Unexpected file operation during backup sync: " + op);
      }
    }
  }

  public static IndexStateClient openRemote(String remoteUri, String indexName, Configuration conf)
      throws IOException {
    IndexStoreConfig.validateRemoteUri(remoteUri);
    return new RemoteStateClient(URI.create(remoteUri), conf, indexName);
  }
}
