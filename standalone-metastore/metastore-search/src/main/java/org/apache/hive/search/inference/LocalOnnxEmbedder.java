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

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.exception.InferenceException;
import org.apache.hive.search.exception.InitializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ONNX Runtime-backed embedding model.
 * Single shared session; {@link #embed} / {@link #embedBatch} share one ONNX run per batch.
 */
public final class LocalOnnxEmbedder implements Embedder {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOnnxEmbedder.class);

  private final String name;
  private final EmbedderSpec modelSpec;
  private final OrtEnvironment ortEnv;
  private final OrtSession session;
  private final HuggingFaceTokenizer tokenizer;
  private final Set<String> sessionInputNames;
  private final String modelOutputName;
  private final Object inferenceLock = new Object();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public LocalOnnxEmbedder(InferenceConfig config) throws InitializeException, IOException {
    EmbedderSpec spec = config.spec();
    this.name = spec.name();
    this.modelSpec = spec;
    this.modelOutputName = spec.modelOutputName();
    Path modelDir = spec.modelDir();
    try {
      this.ortEnv = OrtEnvironment.getEnvironment();
      OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
      opts.setIntraOpNumThreads(Math.max(1, Runtime.getRuntime().availableProcessors()));
      this.session = ortEnv.createSession(
          modelDir.resolve(InferenceConfig.MODEL_ONNX_FILE).toString(), opts);
      this.sessionInputNames = new HashSet<>(session.getInputNames());
      Set<String> outputNames = session.getOutputNames();
      if (!outputNames.contains(modelOutputName)) {
        throw new OrtException(
            "ONNX output '" + modelOutputName + "' is not in the model; available outputs: "
                + String.join(", ", outputNames));
      }
      this.tokenizer = HuggingFaceTokenizer.newInstance(
          modelDir.resolve(InferenceConfig.TOKENIZER));
    } catch (OrtException e) {
      throw InitializeException.wrap("Failed to initialize ONNX embedder", e);
    }
    LOG.info(
        "Loaded ONNX embedding model '{}' from {}, inputs {}, output {}, pooling {}",
        name, modelDir, sessionInputNames, modelOutputName, spec.pooling());
  }

  @Override
  public float[] embed(TaskType task, String text) throws InferenceException {
    return embedBatch(task, new String[] {text})[0];
  }

  @Override
  public float[][] embedBatch(TaskType task, String[] texts) throws InferenceException {
    if (texts.length == 0) {
      return new float[0][];
    }
    ensureOpen();
    for (String text : texts) {
      if (StringUtils.isBlank(text)) {
        throw new InferenceException("Cannot embed null or blank text");
      }
    }
    synchronized (inferenceLock) {
      try {
        return embedBatchLocked(task, texts);
      } catch (OrtException e) {
        throw InferenceException.wrap("Failed to encode text batch", e);
      }
    }
  }

  private float[][] embedBatchLocked(TaskType task, String[] texts) throws OrtException {
    String prefix = modelSpec.prefixFor(task);
    String[] inputs = new String[texts.length];
    for (int i = 0; i < texts.length; i++) {
      inputs[i] = prefix + texts[i];
    }
    Encoding[] encodings = tokenizer.batchEncode(inputs);
    int batchSize = encodings.length;
    long[][] inputIds = new long[batchSize][];
    long[][] attentionMask = new long[batchSize][];
    long[][] tokenTypeIds = new long[batchSize][];
    for (int i = 0; i < batchSize; i++) {
      inputIds[i] = encodings[i].getIds();
      attentionMask[i] = encodings[i].getAttentionMask();
      tokenTypeIds[i] = encodings[i].getTypeIds();
    }

    float[][] pooled;
    Map<String, OnnxTensor> tensors = new HashMap<>();
    try (OnnxTensor idsTensor = createTensor("input_ids", inputIds, tensors);
         OnnxTensor maskTensor = createTensor("attention_mask", attentionMask, tensors);
         OnnxTensor typeTensor = createTensor("token_type_ids", tokenTypeIds, tensors)) {
      if (tensors.isEmpty()) {
        throw new OrtException("ONNX model has no supported inputs: " + sessionInputNames);
      }
      try (OrtSession.Result result = session.run(tensors)) {
        pooled = poolOutput(readModelOutput(result), attentionMask, batchSize);
      }
    }
    float[][] normalized = new float[batchSize][];
    for (int i = 0; i < batchSize; i++) {
      normalized[i] = normalize(pooled[i]);
    }
    return normalized;
  }

  private OnnxTensor createTensor(String inputName, long[][] data,
      Map<String, OnnxTensor> inputs) throws OrtException {
    if (sessionInputNames.contains(inputName)) {
      OnnxTensor onnxTensor = OnnxTensor.createTensor(ortEnv, data);
      inputs.put(inputName, onnxTensor);
      return onnxTensor;
    }
    return null;
  }

  private float[][][] readModelOutput(OrtSession.Result result) throws OrtException {
    var output = result.get(modelOutputName);
    if (output.isEmpty()) {
      throw new OrtException("ONNX result missing output '" + modelOutputName + "'");
    }
    return (float[][][]) output.get().getValue();
  }

  private float[][] poolOutput(float[][][] tokenEmbeddings, long[][] attentionMask, int batchSize) {
    if (tokenEmbeddings.length != batchSize) {
      throw new IllegalStateException(
          "ONNX batch size " + tokenEmbeddings.length + " != " + batchSize);
    }
    float[][] vectors = new float[batchSize][];
    for (int i = 0; i < batchSize; i++) {
      vectors[i] = poolTokenMatrix(tokenEmbeddings[i], attentionMask[i]);
    }
    return vectors;
  }

  private float[] poolTokenMatrix(float[][] tokenRows, long[] attentionMask) {
    if (tokenRows.length == 0) {
      throw new IllegalStateException("ONNX embedding returned zero token rows");
    }
    return switch (modelSpec.pooling()) {
      case MEAN -> meanPool(tokenRows, attentionMask);
      case CLS -> clsPool(tokenRows);
    };
  }

  static float[] clsPool(float[][] tokenRows) {
    return tokenRows[0].clone();
  }

  static float[] meanPool(float[][] vectors, long[] attentionMask) {
    int numVectors = vectors.length;
    int activeTokens = 0;
    int vectorLength = vectors[0].length;
    float[] averagedVector = new float[vectorLength];
    for (int i = 0; i < numVectors; i++) {
      if (attentionMask[i] == 0) {
        continue;
      }
      activeTokens++;
      for (int j = 0; j < vectorLength; j++) {
        averagedVector[j] += vectors[i][j];
      }
    }
    for (int j = 0; j < vectorLength; j++) {
      averagedVector[j] /= activeTokens;
    }
    return averagedVector;
  }

  static float[] normalize(float[] vec) {
    float norm = 0;
    for (float v : vec) {
      norm += v * v;
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0) {
      for (int i = 0; i < vec.length; i++) {
        vec[i] /= norm;
      }
    }
    return vec;
  }

  private void ensureOpen() throws InferenceException {
    if (closed.get()) {
      throw new InferenceException("Embedding model '" + name + "' is closed");
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (tokenizer != null) {
      tokenizer.close();
    }
    if (session != null) {
      try {
        session.close();
      } catch (OrtException e) {
        LOG.warn("Failed to close ONNX session for '{}'", name, e);
      }
    }
    LOG.debug("Closed embedding model '{}'", name);
  }

  @Override
  public String name() {
    return name;
  }
}
