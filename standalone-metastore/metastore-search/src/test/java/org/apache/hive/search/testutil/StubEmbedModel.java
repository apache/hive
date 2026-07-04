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

package org.apache.hive.search.testutil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hive.search.exception.IndexException;
import org.apache.hive.search.inference.EmbedModel;

/** Deterministic embedding model for tests (no ONNX). */
public final class StubEmbedModel implements EmbedModel {
  private static final int DIMENSION = 8;

  private final String name;
  private final AtomicInteger encodeBatchCalls = new AtomicInteger();

  public StubEmbedModel(String name) {
    this.name = name;
  }

  public int encodeBatchCalls() {
    return encodeBatchCalls.get();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public float[] encode(TaskType task, String text) {
    return vector(text + ":" + task.name());
  }

  @Override
  public void close() {
    // no-op
  }

  private static float[] vector(String text) {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    float[] embedding = new float[DIMENSION];
    for (int i = 0; i < DIMENSION; i++) {
      embedding[i] = (bytes[i % bytes.length] & 0xFF) / 255.0f;
    }
    float norm = 0f;
    for (float value : embedding) {
      norm += value * value;
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0f) {
      for (int i = 0; i < embedding.length; i++) {
        embedding[i] /= norm;
      }
    }
    return embedding;
  }

  public static float[] queryVector(String text) {
    return vector(text + ":" + TaskType.QUERY.name());
  }

  @Override
  public float[][] encodeBatch(TaskType task, String[] texts) throws IndexException {
    encodeBatchCalls.incrementAndGet();
    return Arrays.stream(texts).map(text -> encode(task, text)).toArray(float[][]::new);
  }
}
