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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hive.search.exception.InferenceException;

import ai.onnxruntime.OrtException;

/** One embedding job waiting on the shared inference queue. */
final class EmbedRequest {
  static final EmbedRequest SHUTDOWN = new EmbedRequest(null, null);

  private final EmbedModel.TaskType task;
  private final String text;
  private final CountDownLatch done = new CountDownLatch(1);
  private final AtomicReference<float[]> result = new AtomicReference<>();
  private final AtomicReference<Exception> error = new AtomicReference<>();

  EmbedRequest(EmbedModel.TaskType task, String text) {
    this.task = task;
    this.text = text;
  }

  EmbedModel.TaskType task() {
    return task;
  }

  String text() {
    return text;
  }

  boolean isShutdown() {
    return this == SHUTDOWN;
  }

  void complete(float[] vector) {
    result.set(vector);
    done.countDown();
  }

  void fail(Exception failure) {
    error.set(failure);
    done.countDown();
  }

  float[] awaitResult() throws InferenceException {
    try {
      done.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw InferenceException.wrap("Embedding interrupted", e);
    }
    Exception failure = error.get();
    if (failure != null) {
      if (failure instanceof OrtException ort) {
        throw InferenceException.wrap("Failed to encode text", ort);
      }
      throw InferenceException.wrap("Failed to encode text", failure);
    }
    return result.get();
  }
}
