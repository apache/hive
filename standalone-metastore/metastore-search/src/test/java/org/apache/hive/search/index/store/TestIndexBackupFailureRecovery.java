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

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.index.manifest.IndexManifest;
import org.apache.hive.search.testutil.InMemoryIndexStateClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestIndexBackupFailureRecovery {

  private InMemoryIndexStateClient local;
  private InMemoryIndexStateClient remote;

  @Before
  public void setUp() {
    local = new InMemoryIndexStateClient();
    remote = new InMemoryIndexStateClient();
  }

  @Test
  public void restoreResumesAfterPartialFileCopy() throws Exception {
    IndexManifest target = manifest(20L, "segments_1", 100, "segments_2", 200);
    writeManifest(remote, target);
    remote.write("segments_1", new ByteArrayInputStream(new byte[] {1}));
    remote.write("segments_2", new ByteArrayInputStream(new byte[] {2}));

    local.write("segments_1", new ByteArrayInputStream(new byte[] {1}));
    local.writeStagingManifest(target);

    assertTrue(IndexBackupUtils.restoreFromBackup(local, remote));
    assertTrue(local.hasFile("segments_2"));
    assertFalse(local.readStagingManifest().isPresent());
  }

  @Test
  public void resolveInterruptedRestoreKeepsStagingWhenFilesMismatch() throws Exception {
    IndexManifest target = manifest(15L, "segments_1", 100, "segments_2", 200);
    writeManifest(local, manifest(15L, "segments_1", 100));
    local.writeStagingManifest(target);

    IndexBackupUtils.resolveInterruptedRestore(local);

    assertTrue(local.readStagingManifest().isPresent());
  }

  @Test
  public void syncToBackupRemovesStaleRemoteFiles() throws Exception {
    writeManifest(remote, manifest(5L, "segments_old", 50));
    remote.write("segments_old", new ByteArrayInputStream(new byte[] {9}));

    writeManifest(local, manifest(10L, "segments_new", 100));
    local.write("segments_new", new ByteArrayInputStream(new byte[] {1, 2, 3}));

    assertTrue(IndexBackupUtils.syncToBackup(local, remote));
    assertFalse(remote.hasFile("segments_old"));
    assertTrue(remote.hasFile("segments_new"));
  }

  @Test
  public void restoreFromBackupRemovesStaleLocalFiles() throws Exception {
    writeManifest(local, manifest(5L, "segments_old", 50));
    local.write("segments_old", new ByteArrayInputStream(new byte[] {9}));

    writeManifest(remote, manifest(20L, "segments_new", 100));
    remote.write("segments_new", new ByteArrayInputStream(new byte[] {3, 2, 1}));

    assertTrue(IndexBackupUtils.restoreFromBackup(local, remote));
    assertFalse(local.hasFile("segments_old"));
    assertTrue(local.hasFile("segments_new"));
  }

  private static IndexManifest manifest(long eventId, String fileName, long size) {
    return IndexManifest.create(
        "hive_tables",
        List.of(new IndexManifest.IndexFile(fileName, size)),
        "bge-small",
        eventId);
  }

  private static IndexManifest manifest(
      long eventId, String fileName1, long size1, String fileName2, long size2) {
    return IndexManifest.create(
        "hive_tables",
        List.of(
            new IndexManifest.IndexFile(fileName1, size1),
            new IndexManifest.IndexFile(fileName2, size2)),
        "bge-small",
        eventId);
  }

  private static void writeManifest(InMemoryIndexStateClient client, IndexManifest manifest)
      throws Exception {
    client.write(IndexManifest.MANIFEST_FILE_NAME, new ByteArrayInputStream(manifest.toJsonBytes()));
  }
}
