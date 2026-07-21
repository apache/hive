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
import org.apache.hive.search.inference.EmbedderSpec;

public record InferenceConfig(Configuration configuration) {
  public static final String MODEL_ONNX_FILE = "model.onnx";
  public static final String TOKENIZER = "tokenizer.json";

  public static final String EMBEDDER_LOCAL_DIR = "metastore.inference.local.dir";
  public static final String EMBEDDER_LOCAL_DIR_DEFAULT = System.getProperty("java.io.tmpdir") + "/hivesearch-cache";
  public static final String EMBEDDER_REMOTE_DIR = "metastore.inference.remote.dir";

  public static final String EMBEDDER_NAME = "metastore.inference.embedder.name";
  public static final String EMBEDDER_PROMPT_DOC = "metastore.inference.embedder.prompt.doc";
  public static final String EMBEDDER_PROMPT_QUERY = "metastore.inference.embedder.prompt.query";

  /** Token pooling for ONNX output: {@code mean} (default) or {@code cls} (Snowflake Arctic, etc.). */
  public static final String EMBEDDER_POOLING = "metastore.inference.embedder.pooling";

  public EmbedderSpec spec() throws InitializeException, IOException {
    String name = embedderName();
    if (StringUtils.isEmpty(name)) {
      throw new InitializeException("No model configured for embedding the search");
    }
    URI modelPath = URI.create(configuration.get(EMBEDDER_LOCAL_DIR, EMBEDDER_LOCAL_DIR_DEFAULT));
    Path lPath = new Path("file://" + modelPath.getPath());
    if (requireConfigured(lPath, name)) {
      String remotePath = configuration.get(EMBEDDER_REMOTE_DIR);
      String message = "Can't find the model or tokenizer in %s for " + name;
      if (StringUtils.isEmpty(remotePath)) {
        throw new InitializeException(String.format(message, lPath));
      }
      Path rPath = new Path(remotePath);
      if (requireConfigured(rPath, name)) {
        throw new InitializeException(String.format(message, rPath));
      }
      Path dest = new Path(lPath, name);
      Path src = new Path(rPath, name);
      lPath.getFileSystem(configuration).mkdirs(dest);
      FileSystem fileSystem = rPath.getFileSystem(configuration);
      fileSystem.copyToLocalFile(new Path(src, MODEL_ONNX_FILE), new Path(dest, MODEL_ONNX_FILE));
      fileSystem.copyToLocalFile(new Path(src, TOKENIZER), new Path(dest, TOKENIZER));
    }
    return new EmbedderSpec(
        name,
        java.nio.file.Path.of(new Path(lPath, name).toUri()),
        configuration.get(EMBEDDER_PROMPT_DOC, "passage: "),
        configuration.get(EMBEDDER_PROMPT_QUERY, "query: "),
        EmbedderSpec.Pooling.fromConfig(configuration.get(EMBEDDER_POOLING, "mean")));
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

  public String embedderName() {
    return configuration.get(EMBEDDER_NAME);
  }
}
