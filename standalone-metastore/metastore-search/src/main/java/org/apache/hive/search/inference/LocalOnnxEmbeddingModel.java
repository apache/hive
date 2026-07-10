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

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.onnx.OnnxEmbeddingModel;
import dev.langchain4j.model.embedding.onnx.PoolingMode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.exception.IndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LocalOnnxEmbeddingModel implements EmbedModel {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOnnxEmbeddingModel.class);

  private final String name;
  private final OnnxEmbeddingModel model;
  private final EmbeddingPrompt prompt;
  private final ExecutorService inferExecutor;

  public LocalOnnxEmbeddingModel(String name, Path modelDir, EmbeddingPrompt prompt) {
    this.name = name;
    this.prompt = prompt == null ? EmbeddingPrompt.none() : prompt;
    this.inferExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r, "EmbedModel-" + name);
      thread.setDaemon(true);
      return thread;
    });
    this.model = new OnnxEmbeddingModel(
        modelDir.resolve(InferenceConfig.MODEL_ONNX_FILE),
        modelDir.resolve(InferenceConfig.TOKENIZER),
        PoolingMode.MEAN,
        inferExecutor);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public float[] embed(TaskType task, String text) throws IndexException {
    try {
      return model.embed(prompt.prefixFor(task) + text).content().vector();
    } catch (RuntimeException e) {
      throw IndexException.wrap("Failed to encode text with model '" + name + "'", e);
    }
  }

  @Override
  public float[][] embedBatch(TaskType task, String[] texts) throws IndexException {
    try {
      String prefix = prompt.prefixFor(task);
      List<TextSegment> segments = new ArrayList<>(texts.length);
      for (String text : texts) {
        segments.add(TextSegment.from(prefix + text));
      }
      List<Embedding> embeddings = model.embedAll(segments).content();
      float[][] vectors = new float[texts.length][];
      for (int i = 0; i < texts.length; i++) {
        vectors[i] = embeddings.get(i).vector();
      }
      return vectors;
    } catch (RuntimeException e) {
      throw IndexException.wrap("Failed to encode batch with model '" + name + "'", e);
    }
  }

  @Override
  public void close() {
    inferExecutor.shutdown();
    try {
      if (!inferExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        inferExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      inferExecutor.shutdownNow();
    }
    LOG.debug("Closed embedding model '{}'", name);
  }
}
