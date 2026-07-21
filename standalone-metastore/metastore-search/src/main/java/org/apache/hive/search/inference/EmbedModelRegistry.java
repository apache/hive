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

public record EmbedModelRegistry(Map<String, EmbedModel> models) implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(EmbedModelRegistry.class);

  public EmbedModelRegistry(Map<String, EmbedModel> models) {
    this.models = Map.copyOf(models);
  }

  public static EmbedModelRegistry create(Configuration configuration)
      throws InitializeException, IOException {
    InferenceConfig inference = new InferenceConfig(configuration);
    long start = System.currentTimeMillis();
    String modelName = inference.modelName();
    EmbedModel embedModel = new LocalOnnxEmbeddingModel(inference);
    long warmupStart = System.currentTimeMillis();
    try {
      embedModel.embed(EmbedModel.TaskType.QUERY, "warmup");
    } catch (InferenceException e) {
      throw new InitializeException("Failed to warm up embedding model '" + modelName + "'", e);
    }
    LOG.info("Loaded embedding model '{}' in {}ms (warmup {}ms)",
        modelName, System.currentTimeMillis() - start,
        System.currentTimeMillis() - warmupStart);
    return new EmbedModelRegistry(Map.of(modelName, embedModel));
  }

  public EmbedModel get(String modelRef) {
    EmbedModel model = models.get(modelRef);
    if (model == null) {
      throw new IllegalStateException("Embedding model '" + modelRef + "' is not configured");
    }
    return model;
  }

  @Override
  public void close() throws Exception {
    for (EmbedModel model : models.values()) {
      model.close();
    }
  }
}
