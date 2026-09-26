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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestIndexManifest {

  @Test
  public void jsonRoundTrip() throws Exception {
    IndexManifest manifest = IndexManifest.create(
        "hive_tables",
        List.of(
            new IndexManifest.IndexFile("segments_1", 100),
            new IndexManifest.IndexFile("segments_2", 200)),
        "bge-small",
        42L);
    IndexManifest parsed = IndexManifest.fromJson(manifest.toJsonBytes());
    assertEquals(manifest, parsed);
  }

  @Test
  public void sameFilesAsIgnoresOrder() {
    IndexManifest left = IndexManifest.create(
        "idx",
        List.of(
            new IndexManifest.IndexFile("a", 1),
            new IndexManifest.IndexFile("b", 2)),
        "model",
        1L);
    IndexManifest right = IndexManifest.create(
        "idx",
        List.of(
            new IndexManifest.IndexFile("b", 2),
            new IndexManifest.IndexFile("a", 1)),
        "model",
        2L);
    assertTrue(left.sameFilesAs(right));
  }

  @Test
  public void diffDetectsAddsUpdatesAndDeletes() {
    IndexManifest source = IndexManifest.create(
        "idx",
        List.of(
            new IndexManifest.IndexFile("keep", 10),
            new IndexManifest.IndexFile("changed", 20),
            new IndexManifest.IndexFile("added", 30)),
        "model",
        5L);
    IndexManifest target = IndexManifest.create(
        "idx",
        List.of(
            new IndexManifest.IndexFile("keep", 10),
            new IndexManifest.IndexFile("changed", 99),
            new IndexManifest.IndexFile("removed", 40)),
        "model",
        4L);

    List<IndexManifest.ChangedFileOp> ops = source.diff(target);
    assertEquals(3, ops.size());
    assertTrue(ops.contains(IndexManifest.ChangedFileOp.add("changed", 20L, null)));
    assertTrue(ops.contains(IndexManifest.ChangedFileOp.add("added", 30L, null)));
    assertTrue(ops.contains(IndexManifest.ChangedFileOp.del("removed")));
  }

  @Test
  public void diffFromEmptyTargetAddsAllFiles() {
    IndexManifest source = IndexManifest.create(
        "idx",
        List.of(new IndexManifest.IndexFile("segments_1", 100)),
        "model",
        1L);

    List<IndexManifest.ChangedFileOp> ops = source.diff(null);
    assertEquals(1, ops.size());
    assertEquals(new IndexManifest.ChangedFileOp.Add("segments_1", 100L, null), ops.get(0));
  }

  @Test
  public void diffDetectsSameSizeDifferentChecksum() {
    IndexManifest source = IndexManifest.create(
        "idx",
        List.of(new IndexManifest.IndexFile("segments_1", 100, 11L)),
        "model",
        1L);
    IndexManifest target = IndexManifest.create(
        "idx",
        List.of(new IndexManifest.IndexFile("segments_1", 100, 22L)),
        "model",
        1L);
    assertFalse(source.sameFilesAs(target));
    assertEquals(1, source.diff(target).size());
  }

  @Test
  public void diffAgainstIdenticalManifestIsEmpty() {
    IndexManifest manifest = IndexManifest.create(
        "idx",
        List.of(new IndexManifest.IndexFile("segments_1", 100)),
        "model",
        1L);
    assertTrue(manifest.diff(manifest).isEmpty());
  }

  @Test
  public void sameFilesAsReturnsFalseForDifferentSizes() {
    IndexManifest left = IndexManifest.create(
        "idx", List.of(new IndexManifest.IndexFile("a", 1)), "model", 1L);
    IndexManifest right = IndexManifest.create(
        "idx", List.of(new IndexManifest.IndexFile("a", 2)), "model", 1L);
    assertFalse(left.sameFilesAs(right));
  }
}
