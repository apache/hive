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

package org.apache.hive.search.config;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.search.exception.InitializeException;
import org.apache.hive.search.inference.EmbeddingPrompt;

public record InferenceConfig(Configuration configuration) {
  public static final String MODEL_ONNX_FILE = "model.onnx";
  public static final String TOKENIZER = "tokenizer.json";

  public static final String MODEL_LOCAL_DIR = "metastore.inference.local.dir";
  public static final String MODEL_LOCAL_DIR_DEFAULT = System.getProperty("java.io.tmpdir") + "/hivesearch-cache";
  public static final String MODEL_REMOTE_DIR = "metastore.inference.remote.dir";

  public static final String MODEL_NAME = "metastore.inference.embedding.name";
  public static final String EMBEDDING_PROMPT_DOC = "metastore.inference.embedding.prompt.doc";
  public static final String EMBEDDING_PROMPT_QUERY = "metastore.inference.embedding.prompt.query";

  public static final String EMBEDDING_THREADS = "metastore.inference.embedding.threads";

  public static final String EMBEDDING_MAX_SEQ_LENGTH = "metastore.inference.embedding.maxSeqLen";
  public static final int EMBEDDING_MAX_SEQ_LENGTH_DEFAULT = 512;

  static int defaultEmbeddingThreads() {
    return Math.min(4, Math.max(1, Runtime.getRuntime().availableProcessors()));
  }

  public EmbeddingModelSpec embedding() throws InitializeException, IOException {
    String modelName = modelName();
    if (StringUtils.isEmpty(modelName)) {
      throw new InitializeException("No model configured for embedding the search");
    }
    URI modelPath = URI.create(configuration.get(MODEL_LOCAL_DIR, MODEL_LOCAL_DIR_DEFAULT));
    Path lPath = new Path("file://" + modelPath.getPath());
    if (requireConfigured(lPath, modelName)) {
      String remotePath = configuration.get(MODEL_REMOTE_DIR);
      String message = "Can't find the model or tokenizer in %s for " + modelName;
      if (StringUtils.isEmpty(remotePath)) {
        throw new InitializeException(String.format(message, lPath));
      }
      Path rPath = new Path(remotePath);
      if (requireConfigured(rPath, modelName)) {
        throw new InitializeException(String.format(message, rPath));
      }
      Path dest = new Path(lPath, modelName);
      Path src = new Path(rPath, modelName);
      lPath.getFileSystem(configuration).mkdirs(dest);
      FileSystem fileSystem = rPath.getFileSystem(configuration);
      fileSystem.copyToLocalFile(new Path(src, MODEL_ONNX_FILE), new Path(dest, MODEL_ONNX_FILE));
      fileSystem.copyToLocalFile(new Path(src, TOKENIZER), new Path(dest, TOKENIZER));
    }
    EmbeddingPrompt prompt = new EmbeddingPrompt(configuration.get(EMBEDDING_PROMPT_DOC, "passage: "),
        configuration.get(EMBEDDING_PROMPT_QUERY, "query: "));
    return new EmbeddingModelSpec(modelName, new Path(lPath, modelName), prompt);
  }

  private boolean requireConfigured(Path modelPath, String modelName) {
    try {
      FileSystem fileSystem = modelPath.getFileSystem(configuration);
      FileStatus[] fileStatuses = fileSystem.listStatus(new Path(modelPath, modelName), path -> {
        String fileName = path.getName();
        return fileName.equals(MODEL_ONNX_FILE) || fileName.equals(TOKENIZER);
      });
      if (fileStatuses == null || fileStatuses.length != 2) {
        return true;
      }
      return fileStatuses[0].isDirectory() || fileStatuses[1].isDirectory();
    } catch (IOException e) {
      return true;
    }
  }

  public String modelName() {
    return configuration.get(MODEL_NAME);
  }

  public int getEmbeddingThreads() {
    return Math.max(1, configuration.getInt(EMBEDDING_THREADS, defaultEmbeddingThreads()));
  }

  public int getEmbeddingMaxSeqLength() {
    return Math.max(1, configuration.getInt(EMBEDDING_MAX_SEQ_LENGTH, EMBEDDING_MAX_SEQ_LENGTH_DEFAULT));
  }

  public static class EmbeddingModelSpec {
    private final String model;
    private final java.nio.file.Path modelDir;
    private final EmbeddingPrompt prompt;

    public EmbeddingModelSpec(String model, Path path, EmbeddingPrompt promp) {
      this.model = model;
      this.prompt = promp == null ?
          EmbeddingPrompt.none() : promp;
      this.modelDir = java.nio.file.Path.of(path.toUri());
    }

    public String getModel() {
      return model;
    }

    public java.nio.file.Path getModelDir() {
      return modelDir;
    }

    public EmbeddingPrompt getPrompt() {
      return prompt;
    }
  }
}
