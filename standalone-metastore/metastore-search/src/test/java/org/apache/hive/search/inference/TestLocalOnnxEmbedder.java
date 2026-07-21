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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestLocalOnnxEmbedder {

  @Test
  public void clsPoolUsesFirstTokenVector() {
    float[][] tokenEmbeddings = {
        {1f, 2f},
        {9f, 9f}
    };
    assertArrayEquals(new float[] {1f, 2f}, LocalOnnxEmbedder.clsPool(tokenEmbeddings), 0.001f);
  }

  @Test
  public void meanPoolAveragesAllTokenVectors() {
    float[][] tokenEmbeddings = {
        {1f, 0f},
        {3f, 0f},
        {9f, 9f}
    };

    assertArrayEquals(new float[] {13f / 3f, 9f / 3f}, LocalOnnxEmbedder.meanPool(tokenEmbeddings), 0.001f);
  }

  @Test
  public void normalizeProducesUnitVector() {
    float[] vector = {3f, 4f};
    LocalOnnxEmbedder.normalize(vector);
    float norm = 0f;
    for (float v : vector) {
      norm += v * v;
    }
    assertEquals(1f, norm, 0.001f);
    assertEquals(0.6f, vector[0], 0.001f);
    assertEquals(0.8f, vector[1], 0.001f);
  }

  @Test
  public void meanPoolVectorsAveragesChunkEmbeddings() {
    float[][] chunks = {
        {1f, 0f},
        {3f, 0f}
    };

    assertArrayEquals(new float[] {2f, 0f}, LocalOnnxEmbedder.meanPool(chunks), 0.001f);
  }

  @Test
  public void meanPoolVectorsReturnsSingleRowMean() {
    float[][] chunks = {{0.6f, 0.8f}};

    assertArrayEquals(new float[] {0.6f, 0.8f}, LocalOnnxEmbedder.meanPool(chunks), 0.001f);
  }

  @Test
  public void chunkedMeanPoolThenNormalizeProducesUnitVector() {
    float[][] chunks = {
        {1f, 0f},
        {0f, 1f}
    };
    float[] pooled = LocalOnnxEmbedder.meanPool(chunks);
    LocalOnnxEmbedder.normalize(pooled);

    float norm = 0f;
    for (float v : pooled) {
      norm += v * v;
    }
    assertEquals(1f, norm, 0.001f);
    assertEquals(0.7071f, pooled[0], 0.001f);
    assertEquals(0.7071f, pooled[1], 0.001f);
  }
}
