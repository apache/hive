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

import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;

/** Local ONNX embedding model identity, prompts, and token pooling strategy. */
public record EmbedderSpec(
    String name,
    Path modelDir,
    String documentPrefix,
    String queryPrefix,
    Pooling pooling) {

  public EmbedderSpec {
    if (StringUtils.isEmpty(name)) {
      throw new IllegalArgumentException("model name is required");
    }
    if (modelDir == null) {
      throw new IllegalArgumentException("modelDir is required");
    }
    if (pooling == null) {
      pooling = Pooling.MEAN;
    }
  }

  /** How token-level ONNX outputs are reduced to one sentence vector. */
  public enum Pooling {
    /** Average all token embeddings (E5 and many ST exports). */
    MEAN,
    /** First token ([CLS]). */
    CLS;

    public static Pooling fromConfig(String value) {
      if (StringUtils.isBlank(value)) {
        return MEAN;
      }
      return switch (value.trim().toLowerCase()) {
        case "mean", "average" -> MEAN;
        case "cls", "first" -> CLS;
        default -> throw new IllegalArgumentException(
            "Unknown embedding pooling '" + value + "'; use mean or cls");
      };
    }
  }

  public String prefixFor(Embedder.TaskType task) {
    return switch (task) {
      case DOCUMENT -> documentPrefix == null ? "" : documentPrefix;
      case QUERY -> queryPrefix == null ? "" : queryPrefix;
    };
  }

  /** E5-style prefixes for tests and documentation. */
  public static EmbedderSpec e5(String name, Path modelDir) {
    return new EmbedderSpec(name, modelDir, "passage: ", "query: ", Pooling.MEAN);
  }

  public static EmbedderSpec none(String name, Path modelDir) {
    return new EmbedderSpec(name, modelDir, "", "", Pooling.MEAN);
  }
}
