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
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.search.config.InferenceOptions;
import org.apache.hive.search.exception.InferenceException;
import org.apache.hive.search.exception.InitializeException;

/**
 * ONNX Runtime-backed embedding model.
 * Single shared {@link OrtSession}; {@link #embed} / {@link #embedBatch} serialize inference.
 */
public final class LocalOnnxEmbedder implements Embedder {

  private final String name;
  private final EmbedderSpec modelSpec;
  private final OrtEnvironment ortEnv;
  private final OrtSession session;
  private final Object sessionLock = new Object();
  private final String modelOutputName;
  private final HuggingFaceTokenizer tokenizer;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final boolean needsTokenTypeIds;
  private final boolean needsPositionIds;

  public LocalOnnxEmbedder(InferenceOptions config) throws InitializeException, IOException {
    EmbedderSpec spec = config.spec();
    this.name = spec.name();
    this.modelSpec = spec;
    this.modelOutputName = spec.modelOutputName();
    Path modelDir = spec.modelDir();

    OrtSession localSession = null;
    HuggingFaceTokenizer localTokenizer = null;

    try {
      this.ortEnv = OrtEnvironment.getEnvironment();
      try (OrtSession.SessionOptions opts = OnnxSessionOptionsFactory.create(config)) {
        localSession = ortEnv.createSession(
            modelDir.resolve(InferenceOptions.MODEL_ONNX_FILE).toString(), opts);
      }

      Set<String> sessionInputNames = new HashSet<>(localSession.getInputNames());
      Set<String> outputNames = localSession.getOutputNames();
      if (!outputNames.contains(modelOutputName)) {
        throw new OrtException("ONNX output '" + modelOutputName + "' missing from model, expected: "
            + Arrays.toString(outputNames.toArray()));
      }
      if (!sessionInputNames.contains("input_ids")) {
        throw new OrtException("ONNX model missing required input 'input_ids'");
      }
      this.needsTokenTypeIds = sessionInputNames.contains("token_type_ids");
      this.needsPositionIds = sessionInputNames.contains("position_ids");

      Path tokenizerPath = modelDir.resolve(InferenceOptions.TOKENIZER);
      localTokenizer = HuggingFaceTokenizer.builder()
          .optTokenizerPath(tokenizerPath)
          .optPadding(true)
          .optTruncation(true)
          .optMaxLength(config.maxSequenceLength())
          .build();
      this.session = localSession;
      this.tokenizer = localTokenizer;
    } catch (Exception e) {
      if (localTokenizer != null) {
        localTokenizer.close();
      }
      if (localSession != null) {
        try {
          localSession.close();
        } catch (OrtException ignored) {
        }
      }
      throw InitializeException.wrap("Failed to initialize ONNX embedder", e);
    }
  }

  @Override
  public float[] embed(TaskType task, String text) throws InferenceException {
    return embedBatch(task, new String[] {text})[0];
  }

  @Override
  public float[][] embedBatch(TaskType task, String[] texts) throws InferenceException {
    if (texts == null || texts.length == 0) return new float[0][];
    ensureOpen();
    for (String text : texts) {
      if (StringUtils.isBlank(text)) {
        throw new InferenceException("Cannot embed null or blank text");
      }
    }
    try {
      return embedBatchInternal(task, texts);
    } catch (OrtException | IOException e) {
      throw InferenceException.wrap("Failed to encode text batch", e);
    }
  }

  private float[][] embedBatchInternal(TaskType task, String[] texts)
      throws OrtException, IOException, InferenceException {
    String prefix = modelSpec.prefixFor(task);
    String[] inputs = new String[texts.length];
    for (int i = 0; i < texts.length; i++) {
      inputs[i] = prefix.isEmpty() ? texts[i] : prefix + texts[i];
    }

    synchronized (sessionLock) {
      ensureOpen();
      Encoding[] encodings = tokenizer.batchEncode(inputs);
      int batchSize = encodings.length;

      // All encodings in this batch have identical length
      int seqLength = encodings[0].getIds().length;
      long[] inputIds = new long[batchSize * seqLength];
      long[] attentionMask = new long[batchSize * seqLength];
      long[] tokenTypeIds = needsTokenTypeIds ? new long[batchSize * seqLength] : null;
      long[] positionIds = needsPositionIds ? new long[batchSize * seqLength] : null;

      for (int b = 0; b < batchSize; b++) {
        long[] ids = encodings[b].getIds();
        long[] mask = encodings[b].getAttentionMask();
        long[] types = encodings[b].getTypeIds();
        int offset = b * seqLength;

        System.arraycopy(ids, 0, inputIds, offset, seqLength);
        System.arraycopy(mask, 0, attentionMask, offset, seqLength);
        if (needsTokenTypeIds && types != null) {
          System.arraycopy(types, 0, tokenTypeIds, offset, seqLength);
        }
        if (needsPositionIds) {
          writePositionIds(mask, positionIds, offset, seqLength);
        }
      }

      long[] tensorShape = new long[]{batchSize, seqLength};
      Map<String, OnnxTensor> tensorMap = new HashMap<>();
      try (
          OnnxTensor idsTensor = createBufferTensor("input_ids", inputIds, tensorShape, tensorMap);
          OnnxTensor maskTensor = createBufferTensor("attention_mask", attentionMask, tensorShape, tensorMap);
          OnnxTensor typeTensor = createBufferTensor("token_type_ids", tokenTypeIds, tensorShape, tensorMap);
          OnnxTensor posTensor = createBufferTensor("position_ids", positionIds, tensorShape, tensorMap)
      ) {
        try (OrtSession.Result result = session.run(tensorMap)) {
          return processAndPoolOutput(result, attentionMask, batchSize, seqLength);
        }
      }
    }
  }

  private float[][] processAndPoolOutput(OrtSession.Result result,
      long[] flatPoolingMask, int batchSize, int seqLength) throws OrtException, InferenceException {
    OnnxValue value = result.get(modelOutputName)
        .orElseThrow(() -> new InferenceException(
            "ONNX result missing output '" + modelOutputName + "'"));

    if (!(value instanceof OnnxTensor outputTensor)) {
      throw new InferenceException(
          "ONNX output '" + modelOutputName + "' is not an OnnxTensor");
    }

    long[] shape = outputTensor.getInfo().getShape();
    int hiddenDim = validateOutputShape(shape, batchSize, seqLength);

    int expectedElements = batchSize * seqLength * hiddenDim;
    float[] rawOutput = new float[expectedElements];
    FloatBuffer floatBuffer = outputTensor.getFloatBuffer();
    if (floatBuffer == null) {
      throw new InferenceException("ONNX output tensor '" + modelOutputName + "' cannot be converted to FloatBuffer");
    }
    if (floatBuffer.remaining() < expectedElements) {
      throw new OrtException(
          "ONNX output buffer too small: need " + expectedElements + " floats, got "
              + floatBuffer.remaining());
    }
    floatBuffer.get(rawOutput, 0, expectedElements);

    float[][] pooled = new float[batchSize][hiddenDim];
    float[][] tokenRows = new float[seqLength][hiddenDim];
    long[] rowMask = new long[seqLength];
    EmbedderSpec.Pooling pooling = modelSpec.pooling();

    for (int b = 0; b < batchSize; b++) {
      int batchOffset = b * seqLength * hiddenDim;
      int maskOffset = b * seqLength;
      for (int s = 0; s < seqLength; s++) {
        rowMask[s] = flatPoolingMask[maskOffset + s];
        System.arraycopy(rawOutput, batchOffset + s * hiddenDim, tokenRows[s], 0, hiddenDim);
      }
      pooled[b] = poolTokenMatrix(pooling, tokenRows, rowMask);
      normalizeInPlace(pooled[b]);
    }
    return pooled;
  }

  private static int validateOutputShape(long[] shape, int batchSize, int seqLength)
      throws OrtException {
    if (shape.length != 3) {
      throw new OrtException(
          "Expected 3D token embedding output [batch, seq, hidden], got " + Arrays.toString(shape));
    }
    if (shape[0] != batchSize || shape[1] != seqLength) {
      throw new OrtException(
          "ONNX output batch/seq mismatch: shape " + Arrays.toString(shape)
              + " vs batch " + batchSize + " seq " + seqLength);
    }
    if (shape[2] <= 0) {
      throw new OrtException("Invalid hidden dimension in output shape " + Arrays.toString(shape));
    }
    return (int) shape[2];
  }

  private OnnxTensor createBufferTensor(String name, long[] data, long[] shape, Map<String, OnnxTensor> map)
      throws OrtException {
    if (data == null) return null;
    LongBuffer buffer = ByteBuffer.allocateDirect(data.length * Long.BYTES)
        .order(ByteOrder.nativeOrder())
        .asLongBuffer();
    buffer.put(data);
    buffer.flip();
    OnnxTensor tensor = OnnxTensor.createTensor(ortEnv, buffer, shape);
    map.put(name, tensor);
    return tensor;
  }

  /** Position ids for transformer ONNX graphs. */
  static long[] positionIdsFromMask(long[] attentionMask) {
    long[] positionIds = new long[attentionMask.length];
    writePositionIds(attentionMask, positionIds, 0, attentionMask.length);
    return positionIds;
  }

  private static void writePositionIds(long[] mask, long[] positionIds, int offset, int seqLength) {
    long running = 0;
    int limit = Math.min(mask.length, seqLength);
    for (int i = 0; i < limit; i++) {
      positionIds[offset + i] = mask[i] == 0 ? 0 : running++;
    }
    for (int i = limit; i < seqLength; i++) {
      positionIds[offset + i] = 0;
    }
  }

  static float[] poolTokenMatrix(EmbedderSpec.Pooling pooling, float[][] tokenRows,
      long[] attentionMask) throws InferenceException {
    if (tokenRows.length == 0) {
      throw new InferenceException("ONNX embedding returned zero token rows");
    }
    return switch (pooling) {
      case CLS -> tokenRows[0].clone();
      case LAST -> {
        if (attentionMask != null && attentionMask.length != tokenRows.length) {
          throw new InferenceException(
              "attention mask length " + attentionMask.length + " != token rows " + tokenRows.length);
        }
        int last = 0;
        if (attentionMask != null) {
          last = -1;
          for (int i = 0; i < attentionMask.length; i++) {
            if (attentionMask[i] > 0) {
              last = i;
            }
          }
          if (last < 0) {
            throw new InferenceException("attention mask has no active tokens");
          }
        }
        yield tokenRows[last].clone();
      }
      case MEAN -> {
        int vectorLength = tokenRows[0].length;
        float[] averagedVector = new float[vectorLength];
        int activeTokens = 0;
        for (int i = 0; i < tokenRows.length; i++) {
          if (attentionMask != null && attentionMask[i] == 0) {
            continue;
          }
          activeTokens++;
          for (int j = 0; j < vectorLength; j++) {
            averagedVector[j] += tokenRows[i][j];
          }
        }
        if (activeTokens == 0) {
          throw new InferenceException("attention mask has no active tokens");
        }
        for (int j = 0; j < vectorLength; j++) {
          averagedVector[j] /= activeTokens;
        }
        yield averagedVector;
      }
    };
  }

  static void normalizeInPlace(float[] vec) {
    double sum = 0;
    for (float v : vec) sum += (double) v * v;
    float norm = (float) Math.sqrt(sum);
    if (norm > 0 && !Float.isNaN(norm) && !Float.isInfinite(norm)) {
      for (int i = 0; i < vec.length; i++) vec[i] /= norm;
    }
  }

  private void ensureOpen() throws InferenceException {
    if (closed.get()) {
      throw new InferenceException("Model closed");
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    synchronized (sessionLock) {
      if (tokenizer != null) {
        tokenizer.close();
      }
      if (session != null) {
        try {
          session.close();
        } catch (OrtException e) {
          // ignore close failures during shutdown
        }
      }
    }
  }

  @Override
  public String name() {
    return name;
  }
}