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
import java.nio.LongBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 * ONNX Runtime-backed embedding model (langchain4j OnnxBertBiEncoder-style pooling).
 * Single shared session; {@link #embed} is synchronized.
 */
public final class LocalOnnxEmbedder implements Embedder {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOnnxEmbedder.class);

  private final String name;
  private final EmbedderSpec modelSpec;
  private final OrtEnvironment ortEnv;
  private final OrtSession session;
  private final HuggingFaceTokenizer tokenizer;
  private final Set<String> sessionInputNames;
  private final Object inferenceLock = new Object();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public LocalOnnxEmbedder(InferenceConfig config) throws InitializeException, IOException {
    EmbedderSpec spec = config.spec();
    this.name = spec.name();
    this.modelSpec = spec;
    Path modelDir = spec.modelDir();
    try {
      this.ortEnv = OrtEnvironment.getEnvironment();
      OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
      opts.setIntraOpNumThreads(Math.max(1, Runtime.getRuntime().availableProcessors()));
      this.session = ortEnv.createSession(
          modelDir.resolve(InferenceConfig.MODEL_ONNX_FILE).toString(), opts);
      this.sessionInputNames = new HashSet<>(session.getInputNames());
      this.tokenizer = HuggingFaceTokenizer.newInstance(
          modelDir.resolve(InferenceConfig.TOKENIZER),
          Collections.singletonMap("padding", "false"));
    } catch (OrtException e) {
      throw InitializeException.wrap("Failed to initialize ONNX embedder", e);
    }
    LOG.info(
        "Loaded ONNX embedding model '{}' from {}, inputs {}, pooling {}",
        name, modelDir, sessionInputNames, spec.pooling());
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public float[] embed(TaskType task, String text) throws InferenceException {
    ensureOpen();
    requireEmbeddableText(text);
    synchronized (inferenceLock) {
      try {
        return embedLocked(task, text);
      } catch (OrtException e) {
        throw InferenceException.wrap("Failed to encode text", e);
      }
    }
  }

  @Override
  public float[][] embedBatch(TaskType task, String[] texts) throws InferenceException {
    if (texts.length == 0) {
      return new float[0][];
    }
    ensureOpen();
    float[][] vectors = new float[texts.length][];
    for (int i = 0; i < texts.length; i++) {
      vectors[i] = embed(task, texts[i]);
    }
    return vectors;
  }

  private float[] embedLocked(TaskType task, String text) throws OrtException {
    String input = modelSpec.prefixFor(task) + text;
    List<String> tokens = tokenizer.tokenize(input);
    if (tokens.isEmpty()) {
      throw new OrtException("Cannot embed empty text");
    }
    Encoding encoding = tokenizer.encode(input, true, false);
    float[] pooled = embedInternal(encoding.getIds(), encoding.getAttentionMask(), encoding.getIds().length);
    return normalize(pooled);
  }

  static void requireEmbeddableText(String text) throws InferenceException {
    if (text == null || StringUtils.isBlank(text)) {
      throw new InferenceException("Cannot embed null or blank text");
    }
  }

  private float[] embedInternal(long[] inputIds, long[] attentionMask, int seqLen) throws OrtException {
    long[] ids = new long[seqLen];
    long[] mask = new long[seqLen];
    long[] tokenTypeIds = new long[seqLen];
    System.arraycopy(inputIds, 0, ids, 0, seqLen);
    System.arraycopy(attentionMask, 0, mask, 0, seqLen);

    long[] shape = {1, seqLen};
    OnnxTensor idsTensor = null;
    OnnxTensor maskTensor = null;
    OnnxTensor typeTensor = null;
    try {
      Map<String, OnnxTensor> inputs = new HashMap<>();
      if (sessionInputNames.contains("input_ids")) {
        idsTensor = OnnxTensor.createTensor(ortEnv, LongBuffer.wrap(ids), shape);
        inputs.put("input_ids", idsTensor);
      }
      if (sessionInputNames.contains("attention_mask")) {
        maskTensor = OnnxTensor.createTensor(ortEnv, LongBuffer.wrap(mask), shape);
        inputs.put("attention_mask", maskTensor);
      }
      if (sessionInputNames.contains("token_type_ids")) {
        typeTensor = OnnxTensor.createTensor(ortEnv, LongBuffer.wrap(tokenTypeIds), shape);
        inputs.put("token_type_ids", typeTensor);
      }
      if (inputs.isEmpty()) {
        throw new OrtException("ONNX model has no supported inputs: " + sessionInputNames);
      }
      try (OrtSession.Result result = session.run(inputs)) {
        return poolOutput(result.get(0).getValue());
      }
    } finally {
      closeQuietly(typeTensor);
      closeQuietly(maskTensor);
      closeQuietly(idsTensor);
    }
  }

  private static void closeQuietly(OnnxTensor tensor) {
    if (tensor != null) {
      tensor.close();
    }
  }

  private float[] poolOutput(Object value) {
    if (value instanceof float[][][] tokenEmbeddings) {
      return poolTokenMatrix(tokenEmbeddings[0]);
    }
    if (value instanceof float[][] matrix) {
      if (matrix.length == 1) {
        return matrix[0].clone();
      }
      return poolTokenMatrix(matrix);
    }
    if (value instanceof float[] vector) {
      return vector.clone();
    }
    throw new IllegalStateException("Unsupported ONNX embedding output type: " + value.getClass());
  }

  private float[] poolTokenMatrix(float[][] tokenRows) {
    if (tokenRows.length == 0) {
      throw new IllegalStateException("ONNX embedding returned zero token rows");
    }
    return switch (modelSpec.pooling()) {
      case MEAN -> meanPool(tokenRows);
      case CLS -> clsPool(tokenRows);
    };
  }

  static float[] clsPool(float[][] tokenRows) {
    return tokenRows[0].clone();
  }

  static float[] meanPool(float[][] vectors) {
    int numVectors = vectors.length;
    int vectorLength = vectors[0].length;
    float[] averagedVector = new float[vectorLength];
    for (float[] vector : vectors) {
      for (int j = 0; j < vectorLength; j++) {
        averagedVector[j] += vector[j];
      }
    }
    for (int j = 0; j < vectorLength; j++) {
      averagedVector[j] /= numVectors;
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
}
