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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.exception.InferenceException;
import org.apache.hive.search.exception.InitializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ONNX Runtime-backed embedding model for locally deployed sentence-transformer models. */
public final class LocalOnnxEmbeddingModel implements EmbedModel {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOnnxEmbeddingModel.class);

  private final String name;
  private final InferenceWorker[] workers;
  private final BlockingQueue<EmbedRequest> queue = new LinkedBlockingQueue<>();

  public LocalOnnxEmbeddingModel(InferenceConfig config) throws InitializeException, IOException {
    InferenceConfig.EmbeddingModelSpec spec = config.embedding();
    this.name = spec.getModel();
    int threads = config.getEmbeddingThreads();
    this.workers = new InferenceWorker[threads];
    for (int i = 0; i < threads; i++) {
      workers[i] = new InferenceWorker(config, spec, i, queue);
      workers[i].startWorker();
    }
    LOG.info("Loaded ONNX embedding model '{}' from {} with {} worker thread(s), max {} token(s) per chunk",
        name, spec.getModelDir(), threads, config.getEmbeddingMaxSeqLength());
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public float[] embed(TaskType task, String text) throws InferenceException {
    return enqueue(task, text).awaitResult();
  }

  @Override
  public float[][] embedBatch(TaskType task, String[] texts) throws InferenceException {
    if (texts.length == 0) {
      return new float[0][];
    }
    EmbedRequest[] pending = new EmbedRequest[texts.length];
    for (int i = 0; i < texts.length; i++) {
      pending[i] = enqueue(task, texts[i]);
    }
    float[][] vectors = new float[texts.length][];
    for (int i = 0; i < texts.length; i++) {
      vectors[i] = pending[i].awaitResult();
    }
    return vectors;
  }

  private EmbedRequest enqueue(TaskType task, String text) throws InferenceException {
    EmbedRequest request = new EmbedRequest(task, text);
    try {
      queue.put(request);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw InferenceException.wrap("Embedding interrupted for model '" + name + "'", e);
    }
    return request;
  }

  @Override
  public void close() {
    for (int i = 0; i < workers.length; i++) {
      try {
        queue.put(EmbedRequest.SHUTDOWN);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    for (InferenceWorker worker : workers) {
      worker.awaitStop();
    }
    LOG.debug("Closed embedding model '{}'", name);
  }
}
