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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.exception.InitializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Worker thread that pulls embedding jobs from a shared queue. */
final class InferenceWorker extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(InferenceWorker.class);

  private final OrtEnvironment ortEnv;
  private final OrtSession session;
  private final HuggingFaceTokenizer tokenizer;
  private final EmbeddingPrompt prompt;
  private final BlockingQueue<EmbedRequest> queue;
  private final int maxSeqLength;
  private final Set<String> sessionInputNames;

  InferenceWorker(
      InferenceConfig config,
      InferenceConfig.EmbeddingModelSpec spec,
      int workerIndex,
      BlockingQueue<EmbedRequest> queue)
      throws InitializeException, IOException {
    super("EmbedModel-" + spec.getModel() + "-" + workerIndex);
    setDaemon(true);
    this.prompt = spec.getPrompt() == null ? EmbeddingPrompt.none() : spec.getPrompt();
    this.queue = queue;
    this.maxSeqLength = config.getEmbeddingMaxSeqLength();
    int intraOpThreads = Math.max(1,
        Runtime.getRuntime().availableProcessors() / config.getEmbeddingThreads());
    Path modelDir = spec.getModelDir();
    try {
      this.ortEnv = OrtEnvironment.getEnvironment();
      OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
      opts.setIntraOpNumThreads(intraOpThreads);
      this.session = ortEnv.createSession(
          modelDir.resolve(InferenceConfig.MODEL_ONNX_FILE).toString(), opts);
      this.sessionInputNames = new HashSet<>(session.getInputNames());
      LOG.info("ONNX embedding model '{}' worker {} expects inputs {}",
          spec.getModel(), workerIndex, sessionInputNames);
      this.tokenizer = HuggingFaceTokenizer.newInstance(
          modelDir.resolve(InferenceConfig.TOKENIZER));
    } catch (OrtException e) {
      throw InitializeException.wrap("Failed to initialize embedding worker", e);
    }
  }

  void startWorker() {
    start();
  }

  void awaitStop() {
    try {
      join(TimeUnit.SECONDS.toMillis(30));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      interrupt();
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        EmbedRequest request;
        try {
          request = queue.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        if (request.isShutdown()) {
          break;
        }
        try {
          request.complete(embedInternal(request.task(), request.text()));
        } catch (Exception e) {
          request.fail(e);
        }
      }
    } finally {
      closeResources();
    }
  }

  private float[] embedInternal(EmbedModel.TaskType task, String text) throws OrtException {
    String input = prompt.prefixFor(task) + text;
    Encoding encoding = tokenizer.encode(input, true, true);
    long[] inputIds = encoding.getIds();
    long[] attentionMask = encoding.getAttentionMask();

    if (inputIds.length <= maxSeqLength) {
      return normalize(embedInternal(inputIds, attentionMask, 0, inputIds.length));
    }

    int chunkCount = (inputIds.length + maxSeqLength - 1) / maxSeqLength;
    float[][] chunkVectors = new float[chunkCount][];
    for (int chunk = 0; chunk < chunkCount; chunk++) {
      int offset = chunk * maxSeqLength;
      int chunkLen = Math.min(maxSeqLength, inputIds.length - offset);
      chunkVectors[chunk] = normalize(embedInternal(inputIds, attentionMask, offset, chunkLen));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Embedded {} token(s) in {} chunk(s) on worker {}", inputIds.length, chunkCount,
          getName());
    }
    return normalize(meanPoolVectors(chunkVectors));
  }

  private float[] embedInternal(long[] inputIds, long[] attentionMask, int offset, int seqLen)
      throws OrtException {
    long[] ids = new long[seqLen];
    long[] mask = new long[seqLen];
    long[] tokenTypeIds = new long[seqLen];
    System.arraycopy(inputIds, offset, ids, 0, seqLen);
    System.arraycopy(attentionMask, offset, mask, 0, seqLen);

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
        return poolOutput(result.get(0).getValue(), mask, seqLen);
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

  private static float[] poolOutput(Object value, long[] mask, int seqLen) {
    if (value instanceof float[][][] tokenEmbeddings) {
      return meanPool(tokenEmbeddings[0], mask, seqLen);
    }
    if (value instanceof float[][] matrix) {
      if (matrix.length == 1) {
        return matrix[0].clone();
      }
      return meanPool(matrix, mask, seqLen);
    }
    if (value instanceof float[] vector) {
      return vector.clone();
    }
    throw new IllegalStateException("Unsupported ONNX embedding output type: " + value.getClass());
  }

  static float[] meanPool(float[][] tokenEmbeddings, long[] mask, int seqLen) {
    int dim = tokenEmbeddings[0].length;
    float[] result = new float[dim];
    float maskSum = 0;
    for (int i = 0; i < seqLen && i < tokenEmbeddings.length; i++) {
      if (mask[i] == 1) {
        maskSum++;
        for (int j = 0; j < dim; j++) {
          result[j] += tokenEmbeddings[i][j];
        }
      }
    }
    if (maskSum > 0) {
      for (int j = 0; j < dim; j++) {
        result[j] /= maskSum;
      }
    }
    return result;
  }

  static float[] meanPoolVectors(float[][] vectors) {
    if (vectors.length == 1) {
      return vectors[0].clone();
    }
    int dim = vectors[0].length;
    float[] result = new float[dim];
    for (float[] vector : vectors) {
      for (int j = 0; j < dim; j++) {
        result[j] += vector[j];
      }
    }
    for (int j = 0; j < dim; j++) {
      result[j] /= vectors.length;
    }
    return result;
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

  private void closeResources() {
    if (tokenizer != null) {
      tokenizer.close();
    }
    if (session != null) {
      try {
        session.close();
      } catch (OrtException e) {
        LOG.warn("Failed to close ONNX session on worker {}", getName(), e);
      }
    }
  }
}
