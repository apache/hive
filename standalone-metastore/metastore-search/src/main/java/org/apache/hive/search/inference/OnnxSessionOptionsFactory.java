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

import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import org.apache.hive.search.config.InferenceOptions;

final class OnnxSessionOptionsFactory {
  private OnnxSessionOptionsFactory() {}

  static OrtSession.SessionOptions create(InferenceOptions config) throws OrtException {
    OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
    // 1. Enable full ONNX graph optimizations (constant folding, node fusion, etc.)
    opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
    opts.setExecutionMode(OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL);
    int effectiveIntraOpThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
    opts.setIntraOpNumThreads(effectiveIntraOpThreads);
    return opts;
  }
}
