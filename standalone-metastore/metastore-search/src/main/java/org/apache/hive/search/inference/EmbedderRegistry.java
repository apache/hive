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
import org.apache.hive.search.config.InferenceOptions;
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
    InferenceOptions inference = new InferenceOptions(configuration);
    long start = System.currentTimeMillis();
    String modelName = inference.embedderName();
    Embedder embedder = new LocalOnnxEmbedder(inference);
    long warmupStart = System.currentTimeMillis();
    try {
      String repeatText = "warmup";
      String queryText = "What's the weather like today?";
      String docText = "hot and sunny";

      float[] repeatSingle = embedder.embed(Embedder.TaskType.QUERY, repeatText);
      float[][] repeatBatch =
          embedder.embedBatch(Embedder.TaskType.QUERY, new String[] {repeatText, queryText});
      float[] querySingle = embedder.embed(Embedder.TaskType.QUERY, queryText);
      float[] docSingle = embedder.embed(Embedder.TaskType.DOCUMENT, docText);

      float repeatCos = cosineSimilarity(repeatSingle, repeatBatch[0]);
      float queryRepeatCos = cosineSimilarity(querySingle, repeatBatch[1]);
      float crossTaskCos = cosineSimilarity(docSingle, repeatBatch[1]);

      LOG.info(
          "Embedder warmup cosine: repeat single vs batch={} query single vs batch={} "
              + "doc vs query (semantic)={}",
          repeatCos, queryRepeatCos, crossTaskCos);
      if (repeatCos < 0.99f) {
        LOG.warn(
            "Warmup repeatability check below 0.99 ({}); batch and single embed may disagree",
            repeatCos);
      }
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
