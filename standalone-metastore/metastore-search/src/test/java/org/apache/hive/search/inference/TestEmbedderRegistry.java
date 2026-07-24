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

package org.apache.hive.search.inference;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.testutil.StubEmbedder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestEmbedderRegistry {

  @Test
  public void getReturnsConfiguredModel() throws Exception {
    StubEmbedder model = new StubEmbedder("stub-model");
    EmbedderRegistry registry = new EmbedderRegistry(Map.of("stub-model", model));
    assertTrue(registry.get("stub-model") == model);
    registry.close();
  }

  @Test
  public void getThrowsForUnknownModel() {
    EmbedderRegistry registry = new EmbedderRegistry(Map.of());
    IllegalStateException error = assertThrows(IllegalStateException.class, () -> registry.get("missing"));
    assertTrue(error.getMessage().contains("missing"));
  }

  @Test
  public void cosineSimilarityOfIdenticalVectorsIsOne() {
    float[] vector = {0.6f, 0.8f};
    assertEquals(1f, EmbedderRegistry.cosineSimilarity(vector, vector.clone()), 0.0001f);
  }

  @Test
  public void cosineSimilarityOfOrthogonalVectorsIsZero() {
    assertEquals(
        0f,
        EmbedderRegistry.cosineSimilarity(new float[] {1f, 0f}, new float[] {0f, 1f}),
        0.0001f);
  }

  @Test
  public void cosineSimilarityRejectsMismatchedDimensions() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> EmbedderRegistry.cosineSimilarity(new float[] {1f}, new float[] {1f, 2f}));
    assertTrue(error.getMessage().contains("dimensions differ"));
  }

  @Test
  public void cosineSimilarityUsesStubModelEmbeddings() throws Exception {
    StubEmbedder model = new StubEmbedder("stub-model");
    float[] salesQuery = model.embed(Embedder.TaskType.QUERY, "sales");
    float[] salesQueryAgain = model.embed(Embedder.TaskType.QUERY, "sales");
    float[] salesDoc = model.embed(Embedder.TaskType.DOCUMENT, "sales");

    assertEquals(
        1f, EmbedderRegistry.cosineSimilarity(salesQuery, salesQueryAgain), 0.0001f);
    assertTrue(EmbedderRegistry.cosineSimilarity(salesQuery, salesDoc) < 1f);
  }

  @Test
  public void stubModelProducesDeterministicVectors() throws Exception {
    StubEmbedder model = new StubEmbedder("stub-model");
    float[] first = model.embed(Embedder.TaskType.QUERY, "sales");
    float[] second = model.embed(Embedder.TaskType.QUERY, "sales");
    assertArrayEquals(first, second, 0.0001f);

    float[] document = model.embed(Embedder.TaskType.DOCUMENT, "sales");
    assertNotEquals(first[0], document[0], 0.0001f);
  }
}
