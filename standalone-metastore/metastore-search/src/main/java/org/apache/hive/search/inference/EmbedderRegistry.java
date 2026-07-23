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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.exception.InferenceException;
import org.apache.hive.search.exception.InitializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record EmbedderRegistry(Map<String, Embedder> embedders) implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(EmbedderRegistry.class);

  public EmbedderRegistry(Map<String, Embedder> embedders) {
    this.embedders = Map.copyOf(embedders);
  }

  public static EmbedderRegistry create(Configuration configuration)
      throws InitializeException, IOException {
    InferenceConfig inference = new InferenceConfig(configuration);
    long start = System.currentTimeMillis();
    String modelName = inference.embedderName();
    Embedder embedder = new LocalOnnxEmbedder(inference);
    long warmupStart = System.currentTimeMillis();
    try {
      float[] warmupA = embedder.embed(Embedder.TaskType.QUERY, "warmup");
      float[] warmupB = embedder.embed(Embedder.TaskType.QUERY, "Local onnx for embedding query");
      float[][] warmupBatch =
          embedder.embedBatch(
              Embedder.TaskType.QUERY, new String[] {"warmup", "Local onnx for embedding query"});
      LOG.debug(
          "Embedder warmup cosine: same text={} different text={} repeated phrase={}",
          cosineSimilarity(warmupA, warmupBatch[0]),
          cosineSimilarity(warmupA, warmupBatch[1]),
          cosineSimilarity(warmupB, warmupBatch[1]));
    } catch (InferenceException e) {
      throw new InitializeException("Failed to warm up embedder '" + modelName + "'", e);
    }
    LOG.info("Loaded embedder '{}' in {}ms (warmup {}ms)",
        modelName, System.currentTimeMillis() - start, System.currentTimeMillis() - warmupStart);
    return new EmbedderRegistry(Map.of(modelName, embedder));
  }

  public Embedder get(String ref) {
    Embedder embedder = embedders.get(ref);
    if (embedder == null) {
      throw new IllegalStateException("Embedder '" + ref + "' is not configured");
    }
    return embedder;
  }

  /**
   * Cosine similarity in [-1, 1]. Works for arbitrary vectors; for L2-normalized embeddings
   * (as produced by {@link LocalOnnxEmbedder}) this equals the dot product.
   */
  public static float cosineSimilarity(float[] left, float[] right) {
    if (left.length != right.length) {
      throw new IllegalArgumentException(
          "Embedding dimensions differ: " + left.length + " vs " + right.length);
    }
    double dot = 0;
    double normLeft = 0;
    double normRight = 0;
    for (int i = 0; i < left.length; i++) {
      dot += (double) left[i] * right[i];
      normLeft += (double) left[i] * left[i];
      normRight += (double) right[i] * right[i];
    }
    if (normLeft == 0 || normRight == 0) {
      return 0f;
    }
    return (float) (dot / (Math.sqrt(normLeft) * Math.sqrt(normRight)));
  }

  @Override
  public void close() throws Exception {
    for (Embedder embedder : embedders.values()) {
      embedder.close();
    }
  }
}
