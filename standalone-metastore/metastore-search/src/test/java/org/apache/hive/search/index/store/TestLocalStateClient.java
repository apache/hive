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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestLocalStateClient {

  @Test
  public void readManifestSurvivesLaterCommits() throws Exception {
    ByteBuffersDirectory directory = new ByteBuffersDirectory();
    SnapshotDeletionPolicy snapshots =
        new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    IndexWriterConfig config =
        new IndexWriterConfig(new StandardAnalyzer()).setCommitOnClose(false);
    config.setIndexDeletionPolicy(snapshots);
    LocalStateClient client = new LocalStateClient(directory, "test_index");

    try (IndexWriter writer = new IndexWriter(directory, config)) {
      Document docA = new Document();
      docA.add(new StringField("id", "a", Field.Store.YES));
      writer.addDocument(docA);
      writer.setLiveCommitData(Map.of("nid", "10", "embedder", "model-a").entrySet());
      writer.commit();

      IndexCommit pinned = snapshots.snapshot();
      try {
        Document docB = new Document();
        docB.add(new StringField("id", "b", Field.Store.YES));
        writer.addDocument(docB);
        writer.setLiveCommitData(Map.of("nid", "20", "embedder", "model-a").entrySet());
        writer.commit();

        assertEquals(20L, client.readManifest().orElseThrow().lastEventId());
        assertEquals(10L, client.readManifest(Optional.of(pinned)).orElseThrow().lastEventId());
        assertTrue(client.readManifest(Optional.of(pinned)).orElseThrow().sameFilesAs(
            client.readManifest(Optional.of(pinned)).orElseThrow()));
      } finally {
        snapshots.release(pinned);
      }
    }
  }
}
